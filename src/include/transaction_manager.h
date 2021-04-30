#pragma once

#include <atomic>
#include <condition_variable>
#include <unordered_set>

#include "eager_version_manager.h"
#include "lazy_version_manager.h"


// If LAZY_VERSIONING is set to 0 then eager versioning will be used
#define LAZY_VERSIONING 1
// If PESSIMISTIC_CONFLICT_DETECTION is set to 0 then optimistic conflict detection will be used
#define PESSIMISTIC_CONFLICT_DETECTION 1


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
     * @param transaction transaction performing store
     */
    void Store(void *address, Transaction *transaction);

    /**
     * Adds transaction to read set for address
     * @param address
     * @param transaction transaction performing load
     */
    void Load(void *address, Transaction *transaction);

    /**
     * Clean up all memory associated with transaction that commits
     *
     * @param transaction transaction to clean up memory for
     */
    void XEnd(Transaction *transaction);

    /**
     * Clean up all memory associated with transaction that aborts
     *
     * @param transaction transaction to clean up memory for
     */
    void Abort(Transaction *transaction);

private:
    std::atomic<uint64_t> next_txn_id_;
    std::unordered_map<void *, std::unordered_set<Transaction *>> write_sets_;
    std::shared_mutex write_set_mutex_;
    std::unordered_map<void *, std::unordered_set<Transaction *>> read_sets_;
    std::shared_mutex read_set_mutex_;

    std::condition_variable_any read_stall_cv_;

    void LoadStoreHelper(void *address, Transaction *transaction,
                         std::unordered_map<void *, std::unordered_set<Transaction *>> *map, std::shared_mutex *mutex);

    void LoadStoreHelper(void *address, Transaction *transaction,
                         std::unordered_map<void *, std::unordered_set<Transaction *>> *map);

    void CleanUpHelper(const std::unordered_set<void *> &addresses, Transaction *transaction,
                       std::unordered_map<void *, std::unordered_set<Transaction *>> *map, std::shared_mutex *mutex);

    void CleanUpHelper(const std::unordered_set<void *> &addresses, Transaction *transaction,
                       std::unordered_map<void *, std::unordered_set<Transaction *>> *map);
};