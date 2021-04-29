#include <iostream>
#include "include/simulator_main.h"
#include "include/transaction_manager.h"
#include "include/transaction.h"

int main(int argc, char *argv[]) {
    TransactionManager transaction_manager;
    auto txn = transaction_manager.XBegin();
    int test = 42;

    auto loaded = txn.Load(&test);
    std::cout << "Loaded value is " << loaded << std::endl;

    std::cout << "Writing 666 for txn " << txn.GetTransactionId() << std::endl;
    txn.Store(&test, 666);
    std::cout << "Stored value for txn " << txn.GetTransactionId() << " is " << txn.Load(&test) << std::endl;

    auto second_txn = transaction_manager.XBegin();
    std::cout << "Stored value for txn " << second_txn.GetTransactionId() << " is " << second_txn.Load(&test)
              << std::endl;

    std::cout << "Committing txn " << txn.GetTransactionId() << std::endl;
    txn.XEnd();
    std::cout << "Stored value for txn " << second_txn.GetTransactionId() << " is " << second_txn.Load(&test)
              << std::endl;

    second_txn.XEnd();

    return 0;
}