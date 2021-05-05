#include "include/simulator_main.h"

#include <iostream>
#include <thread>
#include <future>

#include "include/transaction_manager.h"
#include "include/transaction.h"
#include "include/abort_exception.h"
#include "include/transaction_memory_test.h"

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
RunAsyncTransactions(TransactionManager *transaction_manager, std::vector<std::function<void(Transaction *)>> funcs) {
    std::vector<std::future<int>> futures;
    futures.reserve(funcs.size());
    size_t aborts = 0;

    auto start = std::chrono::high_resolution_clock::now();
    for (const auto &func : funcs) {
        futures.push_back(std::async(RunTransaction, transaction_manager, func));
    }

    for (auto &future : futures) {
        aborts += future.get();
    }
    return {aborts, static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start).count())};
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
    std::cout << "Read only non conflicting" << std::endl;
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

    size_t aborts = 0;
    size_t time = 0;
    for (int i = 0; i < 10000; i++) {
        auto[new_aborts, new_time] = RunAsyncTransactions(transaction_manager, {read1, read2, read3});
        aborts += new_aborts;
        time += new_time;
    }
    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void ReadOnlyConflicting(TransactionManager *transaction_manager) {
    std::cout << "Read only conflicting" << std::endl;
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

    size_t aborts = 0;
    size_t time = 0;
    for (int i = 0; i < 10000; i++) {
        auto[new_aborts, new_time] = RunAsyncTransactions(transaction_manager, {read1, read2, read3});
        aborts += new_aborts;
        time += new_time;
    }
    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void WriteOnlyNonConflicting(TransactionManager *transaction_manager) {
    std::cout << "Write only non conflicting" << std::endl;
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

    size_t aborts = 0;
    size_t time = 0;
    for (int i = 0; i < 10000; i++) {
        auto[new_aborts, new_time] = RunAsyncTransactions(transaction_manager, {read1, read2, read3});
        aborts += new_aborts;
        time += new_time;
    }
    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void WriteOnlyConflicting(TransactionManager *transaction_manager) {
    std::cout << "Write only conflicting" << std::endl;
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

    size_t aborts = 0;
    size_t time = 0;
    for (int i = 0; i < 10000; i++) {
        auto[new_aborts, new_time] = RunAsyncTransactions(transaction_manager, {read1, read2, read3});
        aborts += new_aborts;
        time += new_time;
    }
    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void ReadWriteNonConflicting(TransactionManager *transaction_manager) {
    std::cout << "Read write non conflicting" << std::endl;
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

    size_t aborts = 0;
    size_t time = 0;
    for (int i = 0; i < 10000; i++) {
        auto[new_aborts, new_time] = RunAsyncTransactions(transaction_manager, {read1, read2, read3});
        aborts += new_aborts;
        time += new_time;
    }
    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

void ReadWriteConflicting(TransactionManager *transaction_manager) {
    std::cout << "Read write conflicting" << std::endl;
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

    size_t aborts = 0;
    size_t time = 0;
    for (int i = 0; i < 10000; i++) {
        auto[new_aborts, new_time] = RunAsyncTransactions(transaction_manager, {read1, read2, read3});
        aborts += new_aborts;
        time += new_time;
    }
    std::cout << "Aborts: " << aborts << std::endl;
    std::cout << "Time (micro seconds): " << time << std::endl;
}

int main(int argc, char *argv[]) {

    TestCorrectness();

    TransactionManager transaction_manager1(true, true);

    std::cout << std::endl << "LAZY VERSIONING and PESSIMISTIC CONFLICT DETECTION" << std::endl;

    ReadOnlyNonConflicting(&transaction_manager1);
    ReadOnlyConflicting(&transaction_manager1);
    WriteOnlyNonConflicting(&transaction_manager1);
    WriteOnlyConflicting(&transaction_manager1);
    ReadWriteNonConflicting(&transaction_manager1);
    ReadWriteConflicting(&transaction_manager1);

    TransactionManager transaction_manager2(true, false);

    std::cout << std::endl << "LAZY VERSIONING and OPTIMISTIC CONFLICT DETECTION" << std::endl;

    ReadOnlyNonConflicting(&transaction_manager2);
    ReadOnlyConflicting(&transaction_manager2);
    WriteOnlyNonConflicting(&transaction_manager2);
    WriteOnlyConflicting(&transaction_manager2);
    ReadWriteNonConflicting(&transaction_manager2);
    ReadWriteConflicting(&transaction_manager2);

    TransactionManager transaction_manager3(false, true);

    std::cout << std::endl << "EAGER VERSIONING and PESSIMISTIC CONFLICT DETECTION" << std::endl;

    ReadOnlyNonConflicting(&transaction_manager3);
    ReadOnlyConflicting(&transaction_manager3);
    WriteOnlyNonConflicting(&transaction_manager3);
    WriteOnlyConflicting(&transaction_manager3);
    ReadWriteNonConflicting(&transaction_manager3);
    ReadWriteConflicting(&transaction_manager3);
}