#include <iostream>
#include "include/transaction_manager.h"

#include "include/transaction.h"
#include "include/invalid_state_exception.h"
#include "include/abort_exception.h"

static constexpr int ABORT_CONFLICTING_EXPRESSION = -1;
static constexpr int STALL_TRANSACTION = 1;

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
        std::unique_lock<std::shared_mutex> exclusive_read_lock(read_set_mutex_);

        // Check for write conflicts - Writer loses
        if (CheckForConflictWithoutLocking(address, write_sets_, transaction)) {
            AbortWithoutLocks(transaction);
            return;
        }
        // Check for read conflicts - Writer loses
        if (CheckForConflictWithoutLocking(address, read_sets_, transaction)) {
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
        Transaction *conflicting_transaction;
        std::unique_lock<std::shared_mutex> exclusive_write_lock(write_set_mutex_);
        if (int conflict_strategy = CheckForConflictWithoutLocking(address, write_sets_, transaction,
                                                                   &conflicting_transaction)) {
            HandlePessimisticReadConflicts(address, transaction, conflict_strategy, conflicting_transaction,
                                           &exclusive_write_lock);
        }
    }
    std::unique_lock<std::shared_mutex> exclusive_write_lock(read_set_mutex_);
    AddTransactionToAddressSetWithoutLocking(address, read_sets_, transaction);
}

void TransactionManager::HandlePessimisticReadConflicts(void *address, Transaction *transaction, int conflict_strategy,
                                                        Transaction *conflicting_transaction,
                                                        std::unique_lock<std::shared_mutex> *exclusive_write_lock) {
    if (conflict_strategy == ABORT_CONFLICTING_EXPRESSION) {
        if (!conflicting_transaction->MarkStalledTransactionAborted(exclusive_write_lock, &read_stall_cv_)) {
            AbortWithoutLocks(transaction);
        }
        read_stall_cv_.notify_all();
    } else if (conflict_strategy == STALL_TRANSACTION) {
        if (transaction->MarkStalled()) {
            read_stall_cv_.wait(*exclusive_write_lock,
                                [&] { return write_sets_.count(address) == 0 || transaction->IsAborted(); });
            if (transaction->IsAborted() || !transaction->MarkUnstalled()) {
                AbortWithoutLocks(transaction);
            }
        } else {
            AbortWithoutLocks(transaction);
        }
    } else {
        std::cerr << "Unknown conflict strategy for read" << std::endl;
    }

}

void TransactionManager::ResolveConflictsAtCommit(Transaction *transaction) {
    if (!use_pessimistic_conflict_detection_) {
        std::unique_lock<std::shared_mutex> exclusive_write_lock(write_set_mutex_);
        std::unique_lock<std::shared_mutex> exclusive_read_lock(read_set_mutex_);

        if (!AbortTransactionsWithConflictsWithoutLocking(write_sets_, transaction)) {
            AbortWithoutLocks(transaction);
            return;
        }

        if (!AbortTransactionsWithConflictsWithoutLocking(read_sets_, transaction)) {
            AbortWithoutLocks(transaction);
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

int TransactionManager::CheckForConflictWithoutLocking(void *address,
                                                       std::unordered_map<void *, TransactionSet> &address_map,
                                                       Transaction *transaction,
                                                       Transaction **conflicting_transaction) {
    if (address_map.count(address) > 0) {
        auto &transaction_set = address_map.at(address);
        std::shared_lock<std::shared_mutex> transaction_set_lock(transaction_set.transaction_mutex_);
        for (auto *other_transaction : transaction_set.transaction_set_) {
            if (other_transaction != transaction) {
                if (conflicting_transaction == nullptr) {
                    return true;
                } else {
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
                    *conflicting_transaction = other_transaction;
                    bool has_lower_priority = other_transaction->GetTransactionId() < transaction->GetTransactionId();
                    bool is_stalled = other_transaction->IsStalled();
                    if (is_stalled) {
                        return ABORT_CONFLICTING_EXPRESSION;
                    } else {
                        return STALL_TRANSACTION;
                    }
                }
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
    // TODO probably unnecessary since we have an exclusive lock on the entire map
    std::unique_lock<std::shared_mutex> transaction_set_lock(transaction_set.transaction_mutex_);
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