#include <cstring>
#include "include/transaction_manager.h"

TransactionManager::TransactionManager() : next_txn_id_(0) {}

uint64_t TransactionManager::xbegin() {
    auto txn_id = next_txn_id_++;
#if LAZY_VERSIONING
    lazy_version_manager_.xbegin(txn_id);
#else
    eager_version_manager_.xbegin(txn_id);
#endif
    return txn_id;
}

void TransactionManager::abort(uint64_t transaction_id) {
#if LAZY_VERSIONING
    lazy_version_manager_.abort(transaction_id);
#else
    eager_version_manager_.abort(transaction_id);
#endif
}


void TransactionManager::xend(uint64_t transaction_id) {
#if LAZY_VERSIONING
    auto[write_buffer, lock] = lazy_version_manager_.GetWriteBuffer(transaction_id);
    for (const auto &write : write_buffer) {
        std::memcpy(write.first, write.second.data_, write.second.size_);
    }
    lazy_version_manager_.xend(transaction_id);
#else
    eager_version_manager_.xend(transaction_id);
#endif
}
