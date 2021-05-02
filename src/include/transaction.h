#pragma once

#include <atomic>
#include <unordered_set>
#include "eager_version_manager.h"
#include "lazy_version_manager.h"
#include "transaction_manager.h"


class TransactionManager;


class Transaction {

public:
    static constexpr int RUNNING = 0;
    static constexpr int COMMITTING = 1;
    static constexpr int ABORTED = 2;
    static constexpr int STALLED = 3;

    explicit Transaction(uint64_t transaction_id, TransactionManager *transaction_manager, bool use_lazy_versioning);

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
        if (state_ == ABORTED) {
            transaction_manager_->Abort(this);
        }
        transaction_manager_->Store(address, this);
        write_set_.emplace(address);
        version_manager_->Store(address, &value, sizeof(T));
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
        if (state_ == ABORTED) {
            transaction_manager_->Abort(this);
        }
        transaction_manager_->Load(address, this);
        read_set_.emplace(address);
        T res;
        // Check if write is in write buffer
        if (version_manager_->GetValue(address, &res)) {
            return res;
        }
        // TODO aborted here then we SHOULD NOT return address... I think?
        return *address;
    }

    /**
     * Abort transaction
     *
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
    uint64_t GetTransactionId() const;

    /**
     * Mark that this transaction has been aborted
     *
     * @return true if the transaction was successfully aborted false otherwise
     */
    bool MarkAborted();

    /**
     * Mark that this transaction has been aborted IFF it's currently stalled
     *
     * @param exclusive_write_lock exclusive lock on write sets
     * @return current state
     */
    bool MarkStalledTransactionAborted(std::unique_lock<std::shared_mutex> *exclusive_write_lock,
                                       std::condition_variable_any *read_stall_cv);

    /**
     * Mark that this transaction is stalled
     *
     * @return true if the transaction was successfully stalled false otherwise
     */
    bool MarkStalled();

    /**
    * Mark that this transaction is unstalled
    *
    * @return true if the transaction was successfully unstalled false otherwise
    */
    bool MarkUnstalled();

    /**
     *
     * @return true if aborted, false otherwise
     */
    bool IsAborted() { return state_ == ABORTED; }

    /**
     *
     * @return true if stalled, false otherwise
     */
    bool IsStalled() { return state_ == STALLED; }

    /**
     *
     * @return Write set of transaction
     */
    const std::unordered_set<void *> &GetWriteSet() const { return write_set_; }

    /**
     *
     * @return Read set of transaction
     */
    const std::unordered_set<void *> &GetReadSet() const { return read_set_; }

private:
    uint64_t transaction_id_;
    TransactionManager *transaction_manager_;
    std::unique_ptr<VersionManager> version_manager_;

    /**
     * Indicates the state of a transaction.
     * 0 - running
     * 1 - committing
     * 2 - aborting
     * TODO try and implement with enum
     */
    std::atomic<int> state_;
    std::unordered_set<void *> write_set_;
    std::unordered_set<void *> read_set_;

    std::condition_variable_any abort_cv_;
};