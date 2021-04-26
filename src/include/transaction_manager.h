#pragma once

#include <atomic>

#include "eager_version_manager.h"
#include "lazy_version_manager.h"

// If LAZY_VERSIONING is set to 0 then eager versioning will be used
#define LAZY_VERSIONING 0

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
    uint64_t xbegin();

    /**
     * Commit memory transaction
     *
     * @param transaction_id id of transaction to commit
     *
     * @throws TransactionAbortException
     */
    void xend(uint64_t transaction_id);

    /**
     * Store value at address for transaction
     *
     * @tparam T type of value
     * @param address location to store value
     * @param value value to store
     * @param transaction_id id of transaction
     *
     * @throws TransactionAbortException
     */
    template<typename T>
    void store(T *address, T value, uint64_t transaction_id) {
#if LAZY_VERSIONING
        lazy_version_manager_.store(address, value, transaction_id);
#else
        eager_version_manager_.store(address, value, transaction_id);
        *address = value;
#endif
    }

    /**
     * Loads value from address for transaction
     *
     * @tparam T type of value
     * @param address location that value is stored
     * @param transaction_id id of transaction
     *
     * @return value stored at address
     *
     * @throws TransactionAbortException
     */
    template<typename T>
    T load(T *address, uint64_t transaction_id) {
#if LAZY_VERSIONING
        T res;
        // Check if write is in write buffer
        if (lazy_version_manager_.GetValue(transaction_id, address, &res)) {
            return res;
        }
#endif
        return *address;
    }


private:
    std::atomic<uint64_t> next_txn_id_;
#if LAZY_VERSIONING
    LazyVersionManager lazy_version_manager_;
#else
    EagerVersionManager eager_version_manager_;
#endif
};