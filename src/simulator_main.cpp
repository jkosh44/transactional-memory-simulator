#include <iostream>
#include <thread>
#include "include/simulator_main.h"
#include "include/transaction_manager.h"
#include "include/transaction.h"

void txn1(int *test, TransactionManager *transaction_manager) {
    bool succeeded = false;
    while (!succeeded) {
        auto transaction = transaction_manager->XBegin();
        auto loaded = transaction.Load(test);
        transaction.Store(test, 666);
        loaded = transaction.Load(test);
        transaction.XEnd();
        succeeded = true;
    }
}

void txn2(int *test, TransactionManager *transaction_manager) {
    bool succeeded = false;
    while (!succeeded) {
        auto transaction = transaction_manager->XBegin();
        transaction.Load(test);
        transaction.Load(test);
        transaction.Load(test);
        transaction.XEnd();
        succeeded = true;
    }
}

int main(int argc, char *argv[]) {
    TransactionManager transaction_manager;
    int test = 42;

    std::thread thread2(txn2, &test, &transaction_manager);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::thread thread1(txn1, &test, &transaction_manager);

    thread1.join();
    thread2.join();

    return 0;
}