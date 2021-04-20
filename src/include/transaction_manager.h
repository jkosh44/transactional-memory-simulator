#pragma once

#include <atomic>

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
        *address = value;
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
        return *address;
    }


private:
    std::atomic<uint64_t> next_txn_id_;
};