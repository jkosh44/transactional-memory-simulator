#include "include/transaction_manager.h"

#include "include/transaction.h"
#include "include/invalid_state_exception.h"
#include "include/abort_exception.h"


TransactionManager::TransactionManager(bool use_lazy_versioning, bool use_pessimistic_conflict_detection)
        : use_lazy_versioning_(use_lazy_versioning),
          use_pessimistic_conflict_detection_(use_pessimistic_conflict_detection),
          next_txn_id_(0) {

    if (!use_lazy_versioning && !use_pessimistic_conflict_detection) {
        throw InvalidStateException("Impossible to have eager data versioning and optimistic conflict detection.");
    }
}

Transaction TransactionManager::XBegin() {
    return Transaction(next_txn_id_++, this, use_lazy_versioning_);
}

void TransactionManager::Store(void *address, Transaction *transaction) {
    if (use_pessimistic_conflict_detection_) {
        std::unique_lock<std::shared_mutex> exclusive_write_lock(write_set_mutex_);

        // Check for write conflicts - Writer loses
        if (CheckForConflictWithoutLocking(address, write_sets_, transaction)) {
            std::unique_lock<std::shared_mutex> exclusive_read_lock(read_set_mutex_);
            AbortWithoutLocks(transaction);
            return;
        }

        // Check for read conflicts - Writer loses
        std::shared_lock<std::shared_mutex> shared_read_lock(read_set_mutex_);
        if (CheckForConflictWithoutLocking(address, read_sets_, transaction)) {
            shared_read_lock.unlock();
            std::unique_lock<std::shared_mutex> exclusive_read_lock(read_set_mutex_);
            AbortWithoutLocks(transaction);
            return;
        }

        AddTransactionToAddressSetWithoutLocking(address, write_sets_, transaction);
    } else {
        std::unique_lock<std::shared_mutex> exclusive_write_lock(write_set_mutex_);
        AddTransactionToAddressSetWithoutLocking(address, write_sets_, transaction);
    }
}

void TransactionManager::Load(void *address, Transaction *transaction) {
    if (use_pessimistic_conflict_detection_) {
        std::unique_lock<std::shared_mutex> exclusive_write_lock(write_set_mutex_);
        while (!HandlePessimisticReadConflicts(address, transaction, &exclusive_write_lock)) {}
    }
    std::unique_lock<std::shared_mutex> exclusive_write_lock(read_set_mutex_);
    AddTransactionToAddressSetWithoutLocking(address, read_sets_, transaction);
}

/* Greedy algorithm to avoid deadlocks on read stalls. T0 is transaction and T1 is
 * other_transaction. Algorithm as described in the lecture notes is below:
 *
 * - If T1 has lower priority than T0 or if T1 is waiting for another transaction, then T1 aborts
 * when conflicting with T0.
 * - If T1 has higher priority than T0 and is not waiting, then T0 waits until T1 commits, aborts,
 * or starts waiting (in which case the first rule is applied).
 *
 * I made the following changes to avoid excessive aborts:
 *
 * - If T1 is waiting for another transaction, then T1 aborts when conflicting with T0.
 * - If T1 is not waiting, then T0 waits until T1 commits, aborts, or starts waiting (in which case
 * the first rule is applied).
 */
bool TransactionManager::HandlePessimisticReadConflicts(void *address, Transaction *transaction,
                                                        std::unique_lock<std::shared_mutex> *exclusive_write_lock) {
    if (write_sets_.count(address) > 0) {
        auto &transaction_set = write_sets_.at(address);
        for (auto *other_transaction : transaction_set.transaction_set_) {
            if (other_transaction != transaction) {
                if (!other_transaction->MarkStalledTransactionAborted(exclusive_write_lock, &read_stall_cv_)) {
                    if (!transaction->MarkStalled()) {
                        std::unique_lock<std::shared_mutex> exclusive_read_lock(read_set_mutex_);
                        AbortWithoutLocks(transaction);
                    }
                    read_stall_cv_.wait(*exclusive_write_lock,
                                        [&] { return write_sets_.count(address) == 0 || transaction->IsAborted(); });
                    if (transaction->IsAborted() || !transaction->MarkUnstalled()) {
                        std::unique_lock<std::shared_mutex> exclusive_read_lock(read_set_mutex_);
                        AbortWithoutLocks(transaction);
                    }
                }
                return false;
            }
        }
    }
    return true;
}

void TransactionManager::ResolveConflictsAtCommit(Transaction *transaction) {
    if (!use_pessimistic_conflict_detection_) {
        {
            std::shared_lock<std::shared_mutex> shared_write_lock(write_set_mutex_);

            if (!AbortTransactionsWithConflictsWithoutLocking(write_sets_, transaction)) {
                shared_write_lock.unlock();
                Abort(transaction);
                return;
            }
        }

        std::shared_lock<std::shared_mutex> shared_read_lock(read_set_mutex_);
        if (!AbortTransactionsWithConflictsWithoutLocking(read_sets_, transaction)) {
            shared_read_lock.unlock();
            Abort(transaction);
            return;
        }
    }
}

void TransactionManager::XEnd(Transaction *transaction) {
    std::unique_lock<std::shared_mutex> exclusive_write_lock(write_set_mutex_);
    std::unique_lock<std::shared_mutex> exclusive_read_lock(read_set_mutex_);

    RemoveTransactionFromAddressSetWithoutLocking(transaction->GetWriteSet(), write_sets_, transaction);
    RemoveTransactionFromAddressSetWithoutLocking(transaction->GetReadSet(), read_sets_, transaction);

    read_stall_cv_.notify_all();
}

void TransactionManager::AbortWithoutLocks(Transaction *transaction) {
    transaction->Abort();
    RemoveTransactionFromAddressSetWithoutLocking(transaction->GetWriteSet(), write_sets_, transaction);
    RemoveTransactionFromAddressSetWithoutLocking(transaction->GetReadSet(), read_sets_, transaction);

    read_stall_cv_.notify_all();
    throw AbortException("Transaction aborted");
}

void TransactionManager::Abort(Transaction *transaction) {
    std::unique_lock<std::shared_mutex> exclusive_write_lock(write_set_mutex_);
    std::unique_lock<std::shared_mutex> exclusive_read_lock(read_set_mutex_);
    AbortWithoutLocks(transaction);
}

bool TransactionManager::CheckForConflictWithoutLocking(void *address,
                                                        std::unordered_map<void *, TransactionSet> &address_map,
                                                        Transaction *transaction) {
    if (address_map.count(address) > 0) {
        auto &transaction_set = address_map.at(address);
        std::shared_lock<std::shared_mutex> transaction_set_lock(transaction_set.transaction_mutex_);
        for (auto *other_transaction : transaction_set.transaction_set_) {
            if (other_transaction != transaction) {
                return true;
            }
        }
    }
    return false;
}

bool TransactionManager::AbortTransactionsWithConflictsWithoutLocking(
        std::unordered_map<void *, TransactionSet> &address_map,
        Transaction *transaction) {
    for (const auto &address : transaction->GetWriteSet()) {
        if (address_map.count(address) > 0) {
            auto &transaction_set = address_map.at(address);
            std::shared_lock<std::shared_mutex> transaction_set_lock(transaction_set.transaction_mutex_);
            for (auto *other_transaction : transaction_set.transaction_set_) {
                if (other_transaction != transaction && !other_transaction->MarkAborted()) {
                    return false;
                }
            }
        }
    }

    return true;
}

void TransactionManager::AddTransactionToAddressSetWithoutLocking(void *address,
                                                                  std::unordered_map<void *, TransactionSet> &address_map,
                                                                  Transaction *transaction) {
    if (address_map.count(address) == 0) {
        address_map.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(address),
                std::forward_as_tuple());
    }
    auto &transaction_set = address_map.at(address);
    transaction_set.transaction_set_.emplace(transaction);
}

void TransactionManager::RemoveTransactionFromAddressSetWithoutLocking(const std::unordered_set<void *> &address_set,
                                                                       std::unordered_map<void *, TransactionSet> &address_map,
                                                                       Transaction *transaction) {
    for (const auto &address : address_set) {
        auto &transaction_set = address_map.at(address);
        bool clean_up = false;
        {
            std::unique_lock<std::shared_mutex> transaction_set_lock(transaction_set.transaction_mutex_);
            transaction_set.transaction_set_.erase(transaction);
            clean_up = transaction_set.transaction_set_.empty();
        }
        if (clean_up) {
            address_map.erase(address);
        }
    }
}