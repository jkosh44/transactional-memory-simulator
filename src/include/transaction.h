#pragma once

#include <atomic>
#include "eager_version_manager.h"
#include "lazy_version_manager.h"

// If LAZY_VERSIONING is set to 0 then eager versioning will be used
#define LAZY_VERSIONING 1
// If PESSIMISTIC_CONFLICT_DETECTION is set to 0 then optimistic conflict detection will be used
#define PESSIMISTIC_CONFLICT_DETECTION 1

class Transaction {

public:
    explicit Transaction(uint64_t transaction_id);

    /**
     * Store value at address for transaction
     *
     * @tparam T type of value
     * @param address location to Store value
     * @param value value to Store
     *
     * @throws TransactionAbortException
     */
    template<typename T>
    void Store(T *address, T value) {
        if(state_ == ABORTED) {
            Abort();
        }
#if LAZY_VERSIONING
        lazy_version_manager_.Store(address, value);
#else
        eager_version_manager_.Store(address, value);
        *address = value;
#endif
    }

    /**
     * Loads value from address for transaction
     *
     * @tparam T type of value
     * @param address location that value is stored
     *
     * @return value stored at address
     *
     * @throws TransactionAbortException
     */
    template<typename T>
    T Load(T *address) {
        if(state_ == ABORTED) {
            Abort();
        }
#if LAZY_VERSIONING
        T res;
        // Check if write is in write buffer
        if (lazy_version_manager_.GetValue(address, &res)) {
            return res;
        }
#endif
        return *address;
    }

    /**
     * Abort transaction
     * @param transaction_id id of transaction
     */
    void Abort();

    /**
    * Commit memory transaction
    *
    * @param transaction_id id of transaction to commit
    *
    * @throws TransactionAbortException
    */
    void XEnd();

    /**
     *
     * @return Transaction Id of transaction
     */
    uint64_t GetTransactionId();

    /**
     * Mark that this transaction has been aborted
     *
     * @return true if the transaction was successfully aborted false otherwise
     */
    bool MarkAborted();

private:
    uint64_t transaction_id_;
#if LAZY_VERSIONING
    LazyVersionManager lazy_version_manager_;
#else
    EagerVersionManager eager_version_manager_;
#endif
    /**
     * Indicates the state of a transaction.
     * 0 - running
     * 1 - committing
     * 2 - aborting
     * TODO try and implement with enum
     */
    std::atomic<int> state_;
    std::condition_variable abort_cv_;

    static constexpr int RUNNING = 0;
    static constexpr int COMMITTING = 1;
    static constexpr int ABORTED = 2;
};