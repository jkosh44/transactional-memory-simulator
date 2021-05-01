#include "include/simulator_main.h"

#include <iostream>
#include <thread>

#include "include/transaction_manager.h"
#include "include/transaction.h"
#include "include/abort_exception.h"

int RunTransaction(TransactionManager *transaction_manager, const std::function<void(Transaction *)> &func) {
    int aborts = 0;
    bool success = false;
    while (!success) {
        Transaction transaction = transaction_manager->XBegin();
        try {
            func(&transaction);
            transaction.XEnd();
            success = true;
        } catch (const AbortException &e) {
            aborts++;
        }
    }
    return aborts;
}

void txn1(int *test, TransactionManager *transaction_manager) {
    auto transaction_func = [&](Transaction *transaction) {
        transaction->Store(test, 666);
        std::this_thread::sleep_for(std::chrono::seconds(4));
    };
    std::cout << RunTransaction(transaction_manager, transaction_func) << std::endl;
}

void txn2(int *test, TransactionManager *transaction_manager) {
    auto transaction_func = [&](Transaction *transaction) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        transaction->Store(test, 999);
    };
    std::cout << RunTransaction(transaction_manager, transaction_func) << std::endl;
}

int main(int argc, char *argv[]) {
    TransactionManager transaction_manager;
    for (int i = 0; i < 100; i++) {
        int test = 42;

        std::thread thread2(txn2, &test, &transaction_manager);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::thread thread1(txn1, &test, &transaction_manager);

        thread1.join();
        thread2.join();
    }
    return 0;
}