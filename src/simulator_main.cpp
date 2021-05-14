#include "include/simulator_main.h"

#include <iostream>
#include <thread>
#include <future>

#include "include/transaction_manager.h"
#include "include/transaction.h"
#include "include/abort_exception.h"
#include "include/transaction_memory_test.h"

static constexpr int READ_CONCURRENT_TRANSACTIONS = 1000;
static constexpr int READ_ITERATIONS = 10;
static constexpr int WRITE_CONCURRENT_TRANSACTIONS = 20;
static constexpr int WRITE_ITERATIONS = 1000;
static constexpr int READ_WRITE_CONCURRENT_TRANSACTIONS = 20;
static constexpr int READ_WRITE_ITERATIONS = 1000;

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

TransactionRunDetails
RunAsyncTransactions(TransactionManager *transaction_manager, std::vector<std::function<void(Transaction *)>> funcs,
                     size_t iterations) {
    size_t aborts = 0;
    size_t time = 0;

    for (int i = 0; i < iterations; i++) {
        std::vector<std::future<int>> futures;
        futures.reserve(funcs.size());
        auto start = std::chrono::high_resolution_clock::now();
        for (const auto &func : funcs) {
            futures.push_back(std::async(RunTransaction, transaction_manager, func));
        }


        for (auto &future : futures) {
            future.wait();
            aborts += future.get();
        }
        time += static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start).count());
    }
    return {aborts, time};
}

/*
 * Adapted from https://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c to generate random map keys
 */
std::string RandomString() {
    size_t length = 100;
    auto randchar = []() -> char {
        const char charset[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

double RandomFloat() {
    return static_cast<double>(rand()) / (static_cast<double>(RAND_MAX / 10000.0));
}


std::unordered_map<std::string, double> GetTestAccounts(size_t size) {
    std::unordered_map<std::string, double> map;

    while (map.size() < size) {
        map[RandomString()] = RandomFloat();
    }

    return map;
}

std::vector<double *> GetAccountAddresses(std::unordered_map<std::string, double> map) {
    std::vector<double *> res;
    res.reserve(map.size());
    for (auto &account : map) {
        res.emplace_back(&account.second);
    }
    return res;
}

void ReadOnlyNonConflicting(TransactionManager *transaction_manager) {
    std::cout << "Read only non conflicting" << std::endl;
    auto accounts_map = GetTestAccounts(2 * READ_CONCURRENT_TRANSACTIONS);
    auto accounts = GetAccountAddresses(accounts_map);
    std::vector<std::function<void(Transaction *)>> funcs;
    funcs.reserve(READ_CONCURRENT_TRANSACTIONS);
    size_t len = accounts.size();
    for (size_t i = 0; i < len - 1; i += 2) {
        funcs.emplace_back([=](Transaction *transaction) {
            auto a = transaction->Load(accounts[i]);
            auto b = transaction->Load(accounts[i + 1]);
        });
    }

    auto[aborts, time] = RunAsyncTransactions(transaction_manager, funcs, READ_ITERATIONS);

    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void ReadOnlyConflicting(TransactionManager *transaction_manager) {
    std::cout << "Read only conflicting" << std::endl;
    auto accounts_map = GetTestAccounts(2 * READ_CONCURRENT_TRANSACTIONS);
    auto accounts = GetAccountAddresses(accounts_map);
    std::vector<std::function<void(Transaction *)>> funcs;
    funcs.reserve(READ_CONCURRENT_TRANSACTIONS);
    for (size_t i = 0; i < accounts.size() - 1; i += 2) {
        funcs.emplace_back([&](Transaction *transaction) {
            for (auto &account : accounts) {
                auto a = transaction->Load(account);
            }
        });
    }

    auto[aborts, time] = RunAsyncTransactions(transaction_manager, funcs, READ_ITERATIONS);

    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void WriteOnlyNonConflicting(TransactionManager *transaction_manager) {
    std::cout << "Write only non conflicting" << std::endl;
    auto accounts_map = GetTestAccounts(2 * WRITE_CONCURRENT_TRANSACTIONS);
    auto accounts = GetAccountAddresses(accounts_map);
    std::vector<std::function<void(Transaction *)>> funcs;
    funcs.reserve(WRITE_CONCURRENT_TRANSACTIONS);
    for (size_t i = 0; i < accounts.size() - 1; i += 2) {
        funcs.emplace_back([=](Transaction *transaction) {
            transaction->Store(accounts[i], RandomFloat());
            transaction->Store(accounts[i + 1], RandomFloat());
        });
    }

    auto[aborts, time] = RunAsyncTransactions(transaction_manager, funcs, WRITE_ITERATIONS);

    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void WriteOnlyConflicting(TransactionManager *transaction_manager) {
    std::cout << "Write only conflicting" << std::endl;
    auto accounts_map = GetTestAccounts(2 * WRITE_CONCURRENT_TRANSACTIONS);
    auto accounts = GetAccountAddresses(accounts_map);
    std::vector<std::function<void(Transaction *)>> funcs;
    funcs.reserve(WRITE_CONCURRENT_TRANSACTIONS);
    for (size_t i = 0; i < accounts.size() - 1; i += 2) {
        funcs.emplace_back([&](Transaction *transaction) {
            for (auto &account : accounts) {
                transaction->Store(account, RandomFloat());
            }
        });
    }

    auto[aborts, time] = RunAsyncTransactions(transaction_manager, funcs, WRITE_ITERATIONS);

    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void ReadWriteNonConflicting(TransactionManager *transaction_manager) {
    std::cout << "Read write non conflicting" << std::endl;

    auto accounts_map = GetTestAccounts(2 * READ_WRITE_CONCURRENT_TRANSACTIONS);
    auto accounts = GetAccountAddresses(accounts_map);
    std::vector<std::function<void(Transaction *)>> funcs;
    funcs.reserve(READ_WRITE_CONCURRENT_TRANSACTIONS);
    for (size_t i = 0; i < accounts.size() - 1; i += 2) {
        funcs.emplace_back([=](Transaction *transaction) {
            double diff = RandomFloat();

            auto a_balance = transaction->Load(accounts[i]);
            transaction->Store(accounts[i], a_balance - diff);

            auto b_balance = transaction->Load(accounts[i + 1]);
            transaction->Store(accounts[i + 1], b_balance + diff);
        });
    }

    auto[aborts, time] = RunAsyncTransactions(transaction_manager, funcs, READ_WRITE_ITERATIONS);

    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void ReadWriteConflicting(TransactionManager *transaction_manager) {
    std::cout << "Read write conflicting" << std::endl;

    auto accounts_map = GetTestAccounts(2 * READ_WRITE_CONCURRENT_TRANSACTIONS);
    auto accounts = GetAccountAddresses(accounts_map);
    std::vector<std::function<void(Transaction *)>> funcs;
    funcs.reserve(READ_WRITE_CONCURRENT_TRANSACTIONS);
    for (size_t i = 0; i < accounts.size() - 1; i += 2) {
        funcs.emplace_back([&](Transaction *transaction) {
            for (size_t j = 0; j < accounts.size() - 1; j += 2) {
                double diff = RandomFloat();

                auto a_balance = transaction->Load(accounts[j]);
                transaction->Store(accounts[j], a_balance - diff);

                auto b_balance = transaction->Load(accounts[j + 1]);
                transaction->Store(accounts[j + 1], b_balance + diff);
            }
        });
    }

    auto[aborts, time] = RunAsyncTransactions(transaction_manager, funcs, READ_WRITE_ITERATIONS);

    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void EmptyWorkload(TransactionManager *transaction_manager, size_t concurrent_transaction, size_t iterations) {
    std::cout << "Empty" << std::endl;

    std::vector<std::function<void(Transaction *)>> funcs;
    funcs.reserve(concurrent_transaction);
    for (size_t i = 0; i < concurrent_transaction; i ++) {
        funcs.emplace_back([&](Transaction *transaction) {});
    }

    auto[aborts, time] = RunAsyncTransactions(transaction_manager, funcs, iterations);

    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

int main(int argc, char *argv[]) {

    TestCorrectness();

    TransactionManager transaction_manager1(true, true);

    std::cout << std::endl << "LAZY VERSIONING and PESSIMISTIC CONFLICT DETECTION" << std::endl;

    ReadOnlyNonConflicting(&transaction_manager1);
    ReadOnlyConflicting(&transaction_manager1);
    EmptyWorkload(&transaction_manager1, READ_CONCURRENT_TRANSACTIONS, READ_ITERATIONS);
    WriteOnlyNonConflicting(&transaction_manager1);
    WriteOnlyConflicting(&transaction_manager1);
    EmptyWorkload(&transaction_manager1, WRITE_CONCURRENT_TRANSACTIONS, WRITE_ITERATIONS);
    ReadWriteNonConflicting(&transaction_manager1);
    ReadWriteConflicting(&transaction_manager1);
    EmptyWorkload(&transaction_manager1, READ_WRITE_CONCURRENT_TRANSACTIONS, READ_WRITE_ITERATIONS);

    TransactionManager transaction_manager2(true, false);

    std::cout << std::endl << "LAZY VERSIONING and OPTIMISTIC CONFLICT DETECTION" << std::endl;

    ReadOnlyNonConflicting(&transaction_manager2);
    ReadOnlyConflicting(&transaction_manager2);
    EmptyWorkload(&transaction_manager2, READ_CONCURRENT_TRANSACTIONS, READ_ITERATIONS);
    WriteOnlyNonConflicting(&transaction_manager2);
    WriteOnlyConflicting(&transaction_manager2);
    ReadWriteNonConflicting(&transaction_manager2);
    ReadWriteConflicting(&transaction_manager2);

    TransactionManager transaction_manager3(false, true);

    std::cout << std::endl << "EAGER VERSIONING and PESSIMISTIC CONFLICT DETECTION" << std::endl;

    ReadOnlyNonConflicting(&transaction_manager3);
    ReadOnlyConflicting(&transaction_manager3);
    EmptyWorkload(&transaction_manager3, READ_CONCURRENT_TRANSACTIONS, READ_ITERATIONS);
    WriteOnlyNonConflicting(&transaction_manager3);
    WriteOnlyConflicting(&transaction_manager3);
    ReadWriteNonConflicting(&transaction_manager3);
    ReadWriteConflicting(&transaction_manager3);
}