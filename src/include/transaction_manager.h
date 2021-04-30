#pragma once

#include <atomic>
#include <unordered_set>

#include "eager_version_manager.h"
#include "lazy_version_manager.h"


class Transaction;


class TransactionManager {

public:
    /**
     * Default constructor for TransactionManager
     */
    TransactionManager();

    /**
     * Begin memory transaction
     * @return transaction id
     */
    Transaction XBegin();

    /**
     * Adds transaction to write set for address
     *
     * @param address location to Store value
     * @param txn_id id of transaction
     */
    void Store(void *address, uint64_t txn_id);

    /**
     * Adds transaction to read set for address
     * @param address
     * @param txn_id
     */
    void Load(void *address, uint64_t txn_id);

    /**
     * Clean up all memory associated with transaction
     *
     * @param transaction transaction to clean up memory for
     */
    void XEnd(const Transaction &transaction);

private:
    std::atomic<uint64_t> next_txn_id_;
    std::unordered_map<void *, std::unordered_set<uint64_t>> write_sets_;
    std::shared_mutex write_set_mutex_;
    std::unordered_map<void *, std::unordered_set<uint64_t>> read_sets_;
    std::shared_mutex read_set_mutex_;

    void LoadStoreHelper(void *address, uint64_t txn_id, std::unordered_map<void *, std::unordered_set<uint64_t>> *map,
                         std::shared_mutex *mutex);

    void CleanUpHelper(const std::unordered_set<void *> &addresses, uint64_t txn_id,
                       std::unordered_map<void *, std::unordered_set<uint64_t>> *map, std::shared_mutex *mutex);
};