#pragma once

#include <atomic>
#include <condition_variable>
#include <unordered_set>

#include "eager_version_manager.h"
#include "lazy_version_manager.h"

class Transaction;

class TransactionManager {

public:

    struct TransactionSet {
        std::unordered_set<Transaction *> transaction_set_;
        std::shared_mutex transaction_mutex_;
    };

    /**
     * Default constructor for TransactionManager
     */
    TransactionManager(bool use_lazy_versioning, bool use_pessimistic_conflict_detection);

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

    void ResolveConflictsAtCommit(Transaction *transaction);

    /**
     * Clean up all memory associated with transaction that commits
     *
     * @param transaction transaction to clean up memory for
     */
    void XEnd(Transaction *transaction);

    /**
     * Clean up all memory associated with transaction that aborts
     * WARNING: MUST BE CALLED WITH EXCLUSIVE LOCKS ON THE READ AND WRITE SETS
     *
     * @param transaction transaction to clean up memory for
     */
    void AbortWithoutLocks(Transaction *transaction);

    /**
    * Clean up all memory associated with transaction that aborts
    *
    * @param transaction transaction to clean up memory for
    */
    void Abort(Transaction *transaction);

private:
    bool use_lazy_versioning_;
    bool use_pessimistic_conflict_detection_;

    std::atomic<uint64_t> next_txn_id_;
    std::unordered_map<void *, TransactionSet> write_sets_;
    std::shared_mutex write_set_mutex_;
    std::unordered_map<void *, TransactionSet> read_sets_;
    std::shared_mutex read_set_mutex_;

    std::condition_variable_any read_stall_cv_;

    /**
     *
     */

    /**
     * Check and see if there's a conflict with the current transaction
     * DO NOT CALL THIS METHOD WITHOUT AN EXCLUSIVE LOCK
     *
     * @param address Address to check for conflicts
     * @param address_map Map of transaction sets to check for conflicts in
     * @param transaction Transaction to check conflicts for
     * @param conflicting_transaction pointer to conflicting transaction
     * @return 0 if there are no conflicts, -1 if caller should abort conflicting transaction in the case of read
     * stalls, 1 if caller should stall in the case of read stalls
     */
    int CheckForConflictWithoutLocking(void *address, std::unordered_map<void *, TransactionSet> &address_map,
                                       Transaction *transaction, Transaction **conflicting_transaction = nullptr);

    /**
     * Abort all transactions that have a conflict with the current transaction
     * DO NOT CALL THIS METHOD WITHOUT AN EXCLUSIVE LOCK
     *
     * @param address_map Map of transaction sets to check for conflicts in
     * @param transaction Transaction to check conflicts for
     * @return true if we were able to successfully abort other transactions false otherwise
     */
    bool AbortTransactionsWithConflictsWithoutLocking(std::unordered_map<void *, TransactionSet> &address_map,
                                                      Transaction *transaction);


    void HandlePessimisticReadConflicts(void *address, Transaction *transaction, int conflict_strategy,
                                        Transaction *conflicting_transaction,
                                        std::unique_lock<std::shared_mutex> *exclusive_write_lock);

    /**
     * Add transaction to set of transactions
     * DO NOT CALL THIS METHOD WITHOUT AN EXCLUSIVE LOCK
     *
     * @param address Address to add
     * @param address_map Map of transaction sets to add address to
     * @param transaction Transaction to add to transaction set
     */
    void
    AddTransactionToAddressSetWithoutLocking(void *address, std::unordered_map<void *, TransactionSet> &address_map,
                                             Transaction *transaction);

    /**
     * Remove transaction from set of transactions
     * DO NOT CALL THIS METHOD WITHOUT AN EXCLUSIVE LOCK
     *
     * @param address_set Set of address from transaction to remove
     * @param address_map Map of addresses to remove address set from
     * @param transaction Transaction to remove
     */
    void RemoveTransactionFromAddressSetWithoutLocking(const std::unordered_set<void *> &address_set,
                                                       std::unordered_map<void *, TransactionSet> &address_map,
                                                       Transaction *transaction);
};