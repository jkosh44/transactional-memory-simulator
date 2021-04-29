#include "include/transaction.h"

Transaction::Transaction(uint64_t transaction_id) : transaction_id_(transaction_id) {}

void Transaction::Abort() {
#if LAZY_VERSIONING
    lazy_version_manager_.Abort();
#else
    eager_version_manager_.Abort();
#endif
}

void Transaction::XEnd() {
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

uint64_t Transaction::GetTransactionId() {
    return transaction_id_;
}
