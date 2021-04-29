#include "include/transaction_manager.h"

TransactionManager::TransactionManager() : next_txn_id_(0) {}

Transaction TransactionManager::XBegin() {
    return Transaction(next_txn_id_++);
}