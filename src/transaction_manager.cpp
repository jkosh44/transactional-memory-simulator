#include "include/transaction_manager.h"

#include "include/transaction.h"

TransactionManager::TransactionManager() : next_txn_id_(0) {}

Transaction TransactionManager::XBegin() {
    return Transaction(next_txn_id_++, this);
}

void TransactionManager::Store(void *address, uint64_t txn_id) {
    LoadStoreHelper(address, txn_id, &write_sets_, &write_set_mutex_);
}

void TransactionManager::Load(void *address, uint64_t txn_id) {
    LoadStoreHelper(address, txn_id, &read_sets_, &read_set_mutex_);
}

void TransactionManager::LoadStoreHelper(void *address, uint64_t txn_id,
                                         std::unordered_map<void *, std::unordered_set<uint64_t>> *map,
                                         std::shared_mutex *mutex) {
    {
        std::shared_lock<std::shared_mutex> shared_lock(*mutex);
        if (map->count(address) > 0) {
            (*map)[address].emplace(txn_id);
            return;
        }
    }
    {
        std::unique_lock<std::shared_mutex> exclusive_lock(*mutex);
        map->emplace(address, txn_id);
    }
}

void TransactionManager::XEnd(const Transaction &transaction) {
    CleanUpHelper(transaction.GetWriteSet(), transaction.GetTransactionId(), &write_sets_, &write_set_mutex_);
    CleanUpHelper(transaction.GetWriteSet(), transaction.GetTransactionId(), &read_sets_, &read_set_mutex_);
}

void TransactionManager::CleanUpHelper(const std::unordered_set<void *> &addresses, uint64_t txn_id,
                                       std::unordered_map<void *, std::unordered_set<uint64_t>> *map,
                                       std::shared_mutex *mutex) {
    std::unordered_set<void *> clean_up_set;
    {
        std::shared_lock<std::shared_mutex> shared_lock(*mutex);
        for (const auto &address : addresses) {
            map->at(address).erase(txn_id);
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
