#include <iostream>
#include "include/simulator_main.h"
#include "include/transaction_manager.h"

int main(int argc, char *argv[]) {
    TransactionManager transaction_manager;
    auto txn_id = transaction_manager.xbegin();
    int test = 42;

    auto loaded = transaction_manager.load(&test, txn_id);
    std::cout << "Loaded value is " << loaded << std::endl;

    transaction_manager.store<int>(&test, 666, txn_id);
    std::cout << "Stored value is " << test << std::endl;

    transaction_manager.xend(txn_id);

    return 0;
}