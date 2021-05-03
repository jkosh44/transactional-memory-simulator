#pragma once

#include <functional>
#include "transaction_manager.h"

struct TransactionRunDetails {
    TransactionRunDetails(size_t aborts, size_t time_taken) : aborts_(aborts), time_taken_(time_taken) {}

    size_t aborts_;
    size_t time_taken_;
};

int main(int argc, char *argv[]);

/**
 * Run a transaction until the transaction is successful
 *
 * @param transaction_manager transaction manager
 * @param func function to run with transaction
 * @return number of aborts
 */
int RunTransaction(TransactionManager *transaction_manager, const std::function<void(Transaction *)> &func);

/**
 * Run a group of transactions asynchronously
 *
 * @param transaction_manager transaction manager
 * @param funcs functions to run asynchronously
 * @return number of aborts and time taken
 */
TransactionRunDetails
RunAsyncTransactions(TransactionManager *transaction_manager, std::vector<std::function<void(Transaction *)>> funcs);

std::unordered_map<std::string, double> GetTestMap();