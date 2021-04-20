#include "include/transaction_manager.h"

TransactionManager::TransactionManager() : next_txn_id_(0) {}

uint64_t TransactionManager::xbegin() {
    return next_txn_id_++;
}

void TransactionManager::xend(uint64_t transaction_id) {}
