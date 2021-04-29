#pragma once

#include <atomic>

#include "eager_version_manager.h"
#include "lazy_version_manager.h"
#include "transaction.h"

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
    Transaction XBegin();


private:
    std::atomic<uint64_t> next_txn_id_;
};