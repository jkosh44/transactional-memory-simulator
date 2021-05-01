#include "include/simulator_main.h"

#include <iostream>
#include <thread>
#include <execinfo.h>
#include <unistd.h>

#include "include/transaction_manager.h"
#include "include/transaction.h"
#include "include/abort_exception.h"
#include "include/invalid_state_exception.h"

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
        } catch (const std::out_of_range &e) {
            std::cerr << "Out of range error: " << e.what() << std::endl;
            void *array[10];
            size_t size;

            // get void*'s for all entries on the stack
            size = backtrace(array, 10);

            // print out all the frames to stderr
            backtrace_symbols_fd(array, size, STDERR_FILENO);
            exit(1);
        }
    }
    return aborts;
}

void txn1(int *test, TransactionManager *transaction_manager) {
    auto transaction_func = [&](Transaction *transaction) {
        transaction->Store(test, 666);
        transaction->Store(test, 666);
        transaction->Store(test, 666);
        transaction->Load(test);
        transaction->Store(test, 666);
        transaction->Store(test, 666);
        transaction->Store(test, 666);
        transaction->Store(test, 666);
        transaction->Store(test, 666);
//        std::this_thread::sleep_for(std::chrono::seconds(4));
    };
    std::cout << RunTransaction(transaction_manager, transaction_func) << std::endl;
}

void txn2(int *test, TransactionManager *transaction_manager) {
    auto transaction_func = [&](Transaction *transaction) {
//        std::this_thread::sleep_for(std::chrono::seconds(2));
        transaction->Store(test, 999);
        transaction->Store(test, 999);
        transaction->Store(test, 999);
        transaction->Store(test, 999);
        transaction->Store(test, 999);
        transaction->Load(test);
        transaction->Store(test, 999);
        transaction->Store(test, 999);
        transaction->Store(test, 999);
        transaction->Store(test, 999);
        transaction->Store(test, 999);
    };
    std::cout << RunTransaction(transaction_manager, transaction_func) << std::endl;
}

int main(int argc, char *argv[]) {
    TransactionManager transaction_manager(false, true);
    for (int i = 0; i < 100000; i++) {
        int test = 42;

        std::thread thread1(txn1, &test, &transaction_manager);
        std::thread thread2(txn2, &test, &transaction_manager);
//        std::this_thread::sleep_for(std::chrono::seconds(1));

        thread1.join();
        thread2.join();

        std::cout << "test: " << test << std::endl;
    }
    return 0;
}