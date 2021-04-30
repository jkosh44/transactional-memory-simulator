#include <iostream>
#include "include/transaction.h"
#include "include/abort_exception.h"

Transaction::Transaction(uint64_t transaction_id) : transaction_id_(transaction_id), aborted_(false), state_(0) {}

void Transaction::Abort() {
#if LAZY_VERSIONING
    lazy_version_manager_.Abort();
#else
    eager_version_manager_.Abort();
#endif
    abort_cv_.notify_all();
    throw AbortException("Transaction aborted");
}

void Transaction::XEnd() {
    int cur_val = RUNNING;
    bool exchanged = state_.compare_exchange_strong(cur_val, COMMITTING);
    if(!exchanged && cur_val == ABORTED) {
        Abort();
    } else if(!exchanged && cur_val == COMMITTING) {
        std::cerr << "Tried to commit an already committing transaction" << std::endl;
    } else if (exchanged) {
#if LAZY_VERSIONING
        auto &write_buffer = lazy_version_manager_.GetWriteBuffer();
        for (const auto &write : write_buffer) {
            std::memcpy(write.first, write.second.data_, write.second.size_);
        }
        lazy_version_manager_.XEnd();
#else
        eager_version_manager_.XEnd();
#endif
    }
}

uint64_t Transaction::GetTransactionId() {
    return transaction_id_;
}

bool Transaction::MarkAborted() {
    int cur_val = RUNNING;
    bool exchanged = state_.compare_exchange_strong(cur_val, ABORTED);
    if(exchanged || cur_val == ABORTED) {
        // Mutex is sort of pointless but just needed for cv API
        std::mutex mutex;
        std::unique_lock<std::mutex> lock(mutex);
        abort_cv_.wait(lock);
    }
    return exchanged || cur_val == ABORTED;
}
