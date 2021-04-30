#include "include/transaction_manager.h"

#include "include/transaction.h"

TransactionManager::TransactionManager() : next_txn_id_(0) {}

Transaction TransactionManager::XBegin() {
    return Transaction(next_txn_id_++, this);
}

void TransactionManager::Store(void *address, Transaction *transaction) {
#if PESSIMISTIC_CONFLICT_DETECTION
    std::unique_lock<std::shared_mutex> exclusive_write_lock(write_set_mutex_);
    std::unique_lock<std::shared_mutex> exclusive_read_lock(read_set_mutex_);

    //Writer loses
    if (read_sets_.count(address) > 0) {
        for (auto *other_transaction : read_sets_.at(address)) {
            if (other_transaction != transaction) {
                transaction->Abort();
                return;
            }
        }
    }

    if (write_sets_.count(address) > 0) {
        for (auto *other_transaction : write_sets_.at(address)) {
            if (other_transaction != transaction) {
                transaction->Abort();
                return;
            }
        }
    }
    LoadStoreHelper(address, transaction, &write_sets_);
#else

    LoadStoreHelper(address, transaction, &write_sets_, &write_set_mutex_);
#endif
}

void TransactionManager::Load(void *address, Transaction *transaction) {
#if PESSIMISTIC_CONFLICT_DETECTION
    std::unique_lock<std::shared_mutex> exclusive_lock(write_set_mutex_);
    // If there is someone other than the reading transaction, stall until no more writes. If two reads are waiting at
    // the same time, then only one can proceed
    if (write_sets_.count(address) > 0 &&
        !(write_sets_.at(address).size() == 1 && write_sets_.at(address).count(transaction) > 0)) {
        read_stall_cv_.wait(exclusive_lock, [&] { return write_sets_.count(address) == 0; });
    }
#else
#endif
    LoadStoreHelper(address, transaction, &read_sets_, &read_set_mutex_);
}

void TransactionManager::LoadStoreHelper(void *address, Transaction *transaction,
                                         std::unordered_map<void *, std::unordered_set<Transaction *>> *map,
                                         std::shared_mutex *mutex) {
    {
        std::shared_lock<std::shared_mutex> shared_lock(*mutex);
        if (map->count(address) > 0) {
            (*map)[address].emplace(transaction);
            return;
        }
    }
    {
        std::unique_lock<std::shared_mutex> exclusive_lock(*mutex);
        map->emplace(
                std::piecewise_construct,
                std::forward_as_tuple(address),
                std::forward_as_tuple());
        (*map)[address].emplace(transaction);
    }
}

void TransactionManager::LoadStoreHelper(void *address, Transaction *transaction,
                                         std::unordered_map<void *, std::unordered_set<Transaction *>> *map) {

    if (map->count(address) == 0) {
        map->emplace(
                std::piecewise_construct,
                std::forward_as_tuple(address),
                std::forward_as_tuple());
    }
    (*map)[address].emplace(transaction);
}

void TransactionManager::XEnd(Transaction *transaction) {
    CleanUpHelper(transaction->GetWriteSet(), transaction, &write_sets_, &write_set_mutex_);
    read_stall_cv_.notify_all();
    CleanUpHelper(transaction->GetReadSet(), transaction, &read_sets_, &read_set_mutex_);
}

void TransactionManager::Abort(Transaction *transaction) {
    CleanUpHelper(transaction->GetWriteSet(), transaction, &write_sets_);
    read_stall_cv_.notify_all();
    CleanUpHelper(transaction->GetReadSet(), transaction, &read_sets_);
}

void TransactionManager::CleanUpHelper(const std::unordered_set<void *> &addresses, Transaction *transaction,
                                       std::unordered_map<void *, std::unordered_set<Transaction *>> *map,
                                       std::shared_mutex *mutex) {
    std::unordered_set<void *> clean_up_set;
    {
        std::shared_lock<std::shared_mutex> shared_lock(*mutex);
        for (const auto &address : addresses) {
            map->at(address).erase(transaction);
            if (map->at(address).empty()) {
                clean_up_set.emplace(address);
            }
        }
    }
    if (!clean_up_set.empty()) {
        std::unique_lock<std::shared_mutex> exclusive_lock(*mutex);
        for (const auto &address : clean_up_set) {
            if (map->at(address).empty()) {
                map->erase(address);
            }
        }
    }

}


void TransactionManager::CleanUpHelper(const std::unordered_set<void *> &addresses, Transaction *transaction,
                                       std::unordered_map<void *, std::unordered_set<Transaction *>> *map) {
    std::unordered_set<void *> clean_up_set;
    for (const auto &address : addresses) {
        map->at(address).erase(transaction);
        if (map->at(address).empty()) {
            clean_up_set.emplace(address);
        }
    }

    if (!clean_up_set.empty()) {
        for (const auto &address : clean_up_set) {
            map->erase(address);
        }
    }
}
