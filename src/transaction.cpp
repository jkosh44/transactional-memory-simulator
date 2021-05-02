#include <iostream>
#include "include/transaction.h"
#include "include/abort_exception.h"
#include "include/invalid_state_exception.h"

Transaction::Transaction(uint64_t transaction_id, TransactionManager *transaction_manager,
                         bool use_lazy_versioning) :
        transaction_id_(transaction_id), transaction_manager_(transaction_manager), state_(0) {
    if (use_lazy_versioning) {
        version_manager_ = std::make_unique<LazyVersionManager>();
    } else {
        version_manager_ = std::make_unique<EagerVersionManager>();
    }
}

void Transaction::Abort() {
    state_ = ABORTED;
    version_manager_->Abort();
    abort_cv_.notify_all();
}

void Transaction::XEnd() {
    int cur_val = RUNNING;
    bool exchanged = state_.compare_exchange_strong(cur_val, COMMITTING);
    if (!exchanged && cur_val == ABORTED) {
        transaction_manager_->Abort(this);
    } else if (!exchanged && cur_val == COMMITTING) {
        std::cerr << "Tried to commit an already committing transaction" << std::endl;
    } else if (exchanged) {
        transaction_manager_->ResolveConflictsAtCommit(this);
        version_manager_->XEnd();
        transaction_manager_->XEnd(this);
    }
}

uint64_t Transaction::GetTransactionId() const {
    return transaction_id_;
}

bool Transaction::MarkAborted() {
    int cur_val = RUNNING;
    bool exchanged = state_.compare_exchange_strong(cur_val, ABORTED);
    return exchanged || cur_val == ABORTED;
}

bool Transaction::MarkStalledTransactionAborted(std::unique_lock<std::shared_mutex> *exclusive_write_lock,
                                                std::condition_variable_any *read_stall_cv) {
    int cur_val = STALLED;
    bool exchanged = state_.compare_exchange_strong(cur_val, ABORTED);
    read_stall_cv->notify_all();
    abort_cv_.wait(*exclusive_write_lock);
    return exchanged;
}

bool Transaction::MarkStalled() {
    int cur_val = RUNNING;
    bool exchanged = state_.compare_exchange_strong(cur_val, STALLED);
    return exchanged;
}

bool Transaction::MarkUnstalled() {
    int cur_val = STALLED;
    bool exchanged = state_.compare_exchange_strong(cur_val, RUNNING);
    return exchanged;
}