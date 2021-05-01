#include <iostream>
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
        std::unique_lock<std::shared_mutex> exclusive_lock(write_set_mutex_);
        // If there is someone other than the reading transaction, stall until no more writes. If two reads are waiting at
        // the same time, then only one can proceed
        if (CheckForConflictWithoutLocking(address, write_sets_, transaction)) {
            read_stall_cv_.wait(exclusive_lock, [&] { return write_sets_.count(address) == 0; });
        }
    }
    std::unique_lock<std::shared_mutex> exclusive_write_lock(read_set_mutex_);
    AddTransactionToAddressSetWithoutLocking(address, read_sets_, transaction);
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
    transaction->Abort();
    RemoveTransactionFromAddressSetWithoutLocking(transaction->GetWriteSet(), write_sets_, transaction);
    RemoveTransactionFromAddressSetWithoutLocking(transaction->GetReadSet(), read_sets_, transaction);

    read_stall_cv_.notify_all();
    throw AbortException("Transaction aborted");
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