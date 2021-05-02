//TODO don't copy all from simulator main

#include <cassert>

#include "include/transaction.h"
#include "include/transaction_manager.h"
#include "include/abort_exception.h"
#include "include/simulator_main.h"

void assert_double_equals(double a, double b, bool use_lazy_versioning, bool use_pessimistic_conflict_detection) {
    if (std::abs(a - b) > 0.01) {
        std::cerr << "Use Lazy Versioning: " << (use_lazy_versioning ? "TRUE" : "FALSE") << std::endl;
        std::cerr << "Use Pessimistic Conflict Detection: " << (use_pessimistic_conflict_detection ? "TRUE" : "FALSE")
                  << std::endl;
        std::cerr << a << " is not equal to " << b << std::endl;
    }
}

void ReadOnlyNonConflictingTest(TransactionManager *transaction_manager, bool use_lazy_versioning,
                                bool use_pessimistic_conflict_detection) {
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

    RunAsyncTransactions(transaction_manager, {read1, read2, read3});

    assert_double_equals(map["Joe"], 666.42, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Mike"], 33.21, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Sam"], 20.14, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Aparna"], 52.37, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Nana"], 100.32, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Popo"], 500.68, use_lazy_versioning, use_pessimistic_conflict_detection);
}

void ReadOnlyConflictingTest(TransactionManager *transaction_manager, bool use_lazy_versioning,
                             bool use_pessimistic_conflict_detection) {
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

    RunAsyncTransactions(transaction_manager, {read1, read2, read3});

    assert_double_equals(map["Joe"], 666.42, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Mike"], 33.21, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Sam"], 20.14, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Aparna"], 52.37, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Nana"], 100.32, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Popo"], 500.68, use_lazy_versioning, use_pessimistic_conflict_detection);
}

void WriteOnlyNonConflictingTest(TransactionManager *transaction_manager, bool use_lazy_versioning,
                                 bool use_pessimistic_conflict_detection) {
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

    RunAsyncTransactions(transaction_manager, {read1, read2, read3});

    assert_double_equals(map["Joe"], 2345.12, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Mike"], 104.21, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Sam"], 123.43, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Aparna"], 203.53, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Nana"], 435.23, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Popo"], 2394.56, use_lazy_versioning, use_pessimistic_conflict_detection);
}

void WriteOnlyConflictingTest(TransactionManager *transaction_manager, bool use_lazy_versioning,
                              bool use_pessimistic_conflict_detection) {
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

    RunAsyncTransactions(transaction_manager, {read1, read2, read3});

    assert_double_equals(map["Joe"], 2345.12, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Mike"], 104.21, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Sam"], 123.43, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Aparna"], 203.53, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Nana"], 435.23, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Popo"], 2394.56, use_lazy_versioning, use_pessimistic_conflict_detection);
}

void ReadWriteNonConflictingTest(TransactionManager *transaction_manager, bool use_lazy_versioning,
                                 bool use_pessimistic_conflict_detection) {
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

    RunAsyncTransactions(transaction_manager, {read1, read2, read3});

    assert_double_equals(map["Joe"], 666.42 - 20.05, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Mike"], 33.21 + 16.73, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Sam"], 20.14 - 5.42, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Aparna"], 52.37 + 20.05, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Nana"], 100.32 - 16.73, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Popo"], 500.68 + 5.42, use_lazy_versioning, use_pessimistic_conflict_detection);
}

void ReadWriteConflictingTest(TransactionManager *transaction_manager, bool use_lazy_versioning,
                              bool use_pessimistic_conflict_detection) {
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

    RunAsyncTransactions(transaction_manager, {read1, read2, read3});

    assert_double_equals(map["Joe"], 666.42 - 3 * 20.05, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Mike"], 33.21 + 3 * 16.73, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Sam"], 20.14 - 3 * 5.42, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Aparna"], 52.37 + 3 * 20.05, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Nana"], 100.32 - 3 * 16.73, use_lazy_versioning, use_pessimistic_conflict_detection);
    assert_double_equals(map["Popo"], 500.68 + 3 * 5.42, use_lazy_versioning, use_pessimistic_conflict_detection);
}

void TestCorrectness() {

    TransactionManager transaction_manager1(true, true);

    ReadOnlyNonConflictingTest(&transaction_manager1, true, true);
    ReadOnlyConflictingTest(&transaction_manager1, true, true);
    WriteOnlyNonConflictingTest(&transaction_manager1, true, true);
    WriteOnlyConflictingTest(&transaction_manager1, true, true);
    ReadWriteNonConflictingTest(&transaction_manager1, true, true);
    ReadWriteConflictingTest(&transaction_manager1, true, true);
    TransactionManager transaction_manager2(true, false);

    ReadOnlyNonConflictingTest(&transaction_manager2, true, false);
    ReadOnlyConflictingTest(&transaction_manager2, true, false);
    WriteOnlyNonConflictingTest(&transaction_manager2, true, false);
    WriteOnlyConflictingTest(&transaction_manager2, true, false);
    ReadWriteNonConflictingTest(&transaction_manager2, true, false);
    ReadWriteConflictingTest(&transaction_manager2, true, false);

    TransactionManager transaction_manager3(false, true);

    ReadOnlyNonConflictingTest(&transaction_manager3, false, true);
    ReadOnlyConflictingTest(&transaction_manager3, false, true);
    WriteOnlyNonConflictingTest(&transaction_manager3, false, true);
    WriteOnlyConflictingTest(&transaction_manager3, false, true);
    ReadWriteNonConflictingTest(&transaction_manager3, false, true);
    ReadWriteConflictingTest(&transaction_manager3, false, true);
}