#include "include/simulator_main.h"

#include <iostream>
#include <thread>
#include <future>

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

int
RunAsyncTransactions(TransactionManager *transaction_manager, std::vector<std::function<void(Transaction *)>> funcs) {
    std::vector<std::future<int>> futures;
    futures.reserve(funcs.size());
    int aborts = 0;
    for (const auto &func : funcs) {
        futures.push_back(std::async(RunTransaction, transaction_manager, func));
    }

    for (auto &future : futures) {
        aborts += future.get();
    }

    return aborts;
}

std::unordered_map<std::string, double> GetTestMap() {
    std::unordered_map<std::string, double> map;
    map["Joe"] = 666.42;
    map["Mike"] = 33.21;
    map["Sam"] = 20.14;
    map["Aparna"] = 52.37;
    map["Nana"] = 100.32;
    map["Popo"] = 500.68;
    return map;
}

void ReadOnlyNonConflicting(TransactionManager *transaction_manager) {
    auto map = GetTestMap();
    auto read1 = [&](Transaction *transaction) {
        auto joe = transaction->Load(&map.find("Joe")->second);
        auto aparna = transaction->Load(&map.find("Aparna")->second);
    };
    auto read2 = [&](Transaction *transaction) {
        auto nana = transaction->Load(&map.find("Nana")->second);
        auto Mike = transaction->Load(&map.find("Mike")->second);
    };
    auto read3 = [&](Transaction *transaction) {
        auto sam = transaction->Load(&map.find("Sam")->second);
        auto popo = transaction->Load(&map.find("Popo")->second);
    };

    int aborts = 0;
    for (int i = 0; i < 1000; i++) {
        aborts += RunAsyncTransactions(transaction_manager, {read1, read2, read3});
    }
    std::cout << aborts << std::endl;
}

void ReadOnlyConflicting(TransactionManager *transaction_manager) {
    auto map = GetTestMap();
    auto read1 = [&](Transaction *transaction) {
        auto joe = transaction->Load(&map.find("Joe")->second);
        auto aparna = transaction->Load(&map.find("Aparna")->second);
        auto nana = transaction->Load(&map.find("Nana")->second);
        auto Mike = transaction->Load(&map.find("Mike")->second);
        auto sam = transaction->Load(&map.find("Sam")->second);
        auto popo = transaction->Load(&map.find("Popo")->second);
    };
    auto read2 = [&](Transaction *transaction) {
        auto nana = transaction->Load(&map.find("Nana")->second);
        auto Mike = transaction->Load(&map.find("Mike")->second);
        auto joe = transaction->Load(&map.find("Joe")->second);
        auto aparna = transaction->Load(&map.find("Aparna")->second);
        auto sam = transaction->Load(&map.find("Sam")->second);
        auto popo = transaction->Load(&map.find("Popo")->second);
    };
    auto read3 = [&](Transaction *transaction) {
        auto sam = transaction->Load(&map.find("Sam")->second);
        auto popo = transaction->Load(&map.find("Popo")->second);
        auto nana = transaction->Load(&map.find("Nana")->second);
        auto Mike = transaction->Load(&map.find("Mike")->second);
        auto joe = transaction->Load(&map.find("Joe")->second);
        auto aparna = transaction->Load(&map.find("Aparna")->second);
    };

    int aborts = 0;
    for (int i = 0; i < 1000; i++) {
        aborts += RunAsyncTransactions(transaction_manager, {read1, read2, read3});
    }
    std::cout << aborts << std::endl;
}

void WriteOnlyNonConflicting(TransactionManager *transaction_manager) {
    auto map = GetTestMap();
    auto read1 = [&](Transaction *transaction) {
        transaction->Store(&map.find("Joe")->second, 2345.12);
        transaction->Store(&map.find("Aparna")->second, 203.53);
    };
    auto read2 = [&](Transaction *transaction) {
        transaction->Store(&map.find("Nana")->second, 435.23);
        transaction->Store(&map.find("Mike")->second, 104.21);
    };
    auto read3 = [&](Transaction *transaction) {
        transaction->Store(&map.find("Sam")->second, 123.43);
        transaction->Store(&map.find("Popo")->second, 2394.56);
    };

    int aborts = 0;
    for (int i = 0; i < 1000; i++) {
        aborts += RunAsyncTransactions(transaction_manager, {read1, read2, read3});
    }
    std::cout << aborts << std::endl;
}

void WriteOnlyConflicting(TransactionManager *transaction_manager) {
    auto map = GetTestMap();
    auto read1 = [&](Transaction *transaction) {
        transaction->Store(&map.find("Joe")->second, 2345.12);
        transaction->Store(&map.find("Aparna")->second, 203.53);
        transaction->Store(&map.find("Nana")->second, 435.23);
        transaction->Store(&map.find("Mike")->second, 104.21);
        transaction->Store(&map.find("Sam")->second, 123.43);
        transaction->Store(&map.find("Popo")->second, 2394.56);
    };
    auto read2 = [&](Transaction *transaction) {
        transaction->Store(&map.find("Joe")->second, 2345.12);
        transaction->Store(&map.find("Aparna")->second, 203.53);
        transaction->Store(&map.find("Nana")->second, 435.23);
        transaction->Store(&map.find("Mike")->second, 104.21);
        transaction->Store(&map.find("Sam")->second, 123.43);
        transaction->Store(&map.find("Popo")->second, 2394.56);
    };
    auto read3 = [&](Transaction *transaction) {
        transaction->Store(&map.find("Sam")->second, 123.43);
        transaction->Store(&map.find("Popo")->second, 2394.56);
        transaction->Store(&map.find("Nana")->second, 435.23);
        transaction->Store(&map.find("Mike")->second, 104.21);
        transaction->Store(&map.find("Joe")->second, 2345.12);
        transaction->Store(&map.find("Aparna")->second, 203.53);
    };

    int aborts = 0;
    for (int i = 0; i < 1000; i++) {
        aborts += RunAsyncTransactions(transaction_manager, {read1, read2, read3});
    }
    std::cout << aborts << std::endl;
}

void ReadWriteNonConflicting(TransactionManager *transaction_manager) {
    auto map = GetTestMap();
    auto read1 = [&](Transaction *transaction) {
        double diff = 20.05;
        auto joe_balance = transaction->Load(&map.find("Joe")->second);
        transaction->Store(&map.find("Joe")->second, joe_balance - diff);

        auto aparna_balance = transaction->Load(&map.find("Aparna")->second);
        transaction->Store(&map.find("Aparna")->second, aparna_balance + diff);
    };
    auto read2 = [&](Transaction *transaction) {
        double diff = 16.73;
        auto nana_balance = transaction->Load(&map.find("Nana")->second);
        transaction->Store(&map.find("Nana")->second, nana_balance - diff);

        auto mike_balance = transaction->Load(&map.find("Mike")->second);
        transaction->Store(&map.find("Mike")->second, mike_balance + diff);
    };
    auto read3 = [&](Transaction *transaction) {
        double diff = 5.42;
        auto sam_balance = transaction->Load(&map.find("Sam")->second);
        transaction->Store(&map.find("Sam")->second, sam_balance - diff);

        auto popo_balance = transaction->Load(&map.find("Popo")->second);
        transaction->Store(&map.find("Popo")->second, popo_balance + diff);
    };

    int aborts = 0;
    for (int i = 0; i < 1000; i++) {
        aborts += RunAsyncTransactions(transaction_manager, {read1, read2, read3});
    }
    std::cout << aborts << std::endl;
}

void ReadWriteConflicting(TransactionManager *transaction_manager) {
    auto map = GetTestMap();
    auto read1 = [&](Transaction *transaction) {
        double diff = 20.05;
        auto joe_balance = transaction->Load(&map.find("Joe")->second);
        transaction->Store(&map.find("Joe")->second, joe_balance - diff);

        auto aparna_balance = transaction->Load(&map.find("Aparna")->second);
        transaction->Store(&map.find("Aparna")->second, aparna_balance + diff);

        diff = 16.73;
        auto nana_balance = transaction->Load(&map.find("Nana")->second);
        transaction->Store(&map.find("Nana")->second, nana_balance - diff);

        auto mike_balance = transaction->Load(&map.find("Mike")->second);
        transaction->Store(&map.find("Mike")->second, mike_balance + diff);

        diff = 5.42;
        auto sam_balance = transaction->Load(&map.find("Sam")->second);
        transaction->Store(&map.find("Sam")->second, sam_balance - diff);

        auto popo_balance = transaction->Load(&map.find("Popo")->second);
        transaction->Store(&map.find("Popo")->second, popo_balance + diff);
    };
    auto read2 = [&](Transaction *transaction) {
        double diff = 16.73;
        auto nana_balance = transaction->Load(&map.find("Nana")->second);
        transaction->Store(&map.find("Nana")->second, nana_balance - diff);

        auto mike_balance = transaction->Load(&map.find("Mike")->second);
        transaction->Store(&map.find("Mike")->second, mike_balance + diff);

        diff = 20.05;
        auto joe_balance = transaction->Load(&map.find("Joe")->second);
        transaction->Store(&map.find("Joe")->second, joe_balance - diff);

        auto aparna_balance = transaction->Load(&map.find("Aparna")->second);
        transaction->Store(&map.find("Aparna")->second, aparna_balance + diff);

        diff = 5.42;
        auto sam_balance = transaction->Load(&map.find("Sam")->second);
        transaction->Store(&map.find("Sam")->second, sam_balance - diff);

        auto popo_balance = transaction->Load(&map.find("Popo")->second);
        transaction->Store(&map.find("Popo")->second, popo_balance + diff);
    };
    auto read3 = [&](Transaction *transaction) {
        double diff = 5.42;
        auto sam_balance = transaction->Load(&map.find("Sam")->second);
        transaction->Store(&map.find("Sam")->second, sam_balance - diff);

        auto popo_balance = transaction->Load(&map.find("Popo")->second);
        transaction->Store(&map.find("Popo")->second, popo_balance + diff);

        diff = 16.73;
        auto nana_balance = transaction->Load(&map.find("Nana")->second);
        transaction->Store(&map.find("Nana")->second, nana_balance - diff);

        auto mike_balance = transaction->Load(&map.find("Mike")->second);
        transaction->Store(&map.find("Mike")->second, mike_balance + diff);

        diff = 20.05;
        auto joe_balance = transaction->Load(&map.find("Joe")->second);
        transaction->Store(&map.find("Joe")->second, joe_balance - diff);

        auto aparna_balance = transaction->Load(&map.find("Aparna")->second);
        transaction->Store(&map.find("Aparna")->second, aparna_balance + diff);
    };

    int aborts = 0;
    for (int i = 0; i < 1000; i++) {
        aborts += RunAsyncTransactions(transaction_manager, {read1, read2, read3});
    }
    std::cout << aborts << std::endl;
}

int main(int argc, char *argv[]) {
    TransactionManager transaction_manager(false, true);

    ReadOnlyNonConflicting(&transaction_manager);
    ReadOnlyConflicting(&transaction_manager);
    WriteOnlyNonConflicting(&transaction_manager);
    WriteOnlyConflicting(&transaction_manager);
    ReadWriteNonConflicting(&transaction_manager);
    ReadWriteConflicting(&transaction_manager);
}