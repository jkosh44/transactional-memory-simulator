#pragma once

#include <shared_mutex>
#include <unordered_map>
#include <cstring>


struct Undo {
    Undo(void *data, size_t size) : data_(data), size_(size) {}

    void *data_;
    size_t size_;
};


using UndoLog = std::unordered_map<void *, Undo>;

class EagerVersionManager {
public:
    /**
    * Begin transaction with Eager Version Manager
    *
    * @param transaction_id
    */
    void xbegin(uint64_t transaction_id);

    /**
     * Store undo into undo buffer
     *
     * @tparam T type of value to undo
     * @param address address to undo
     * @param value value to undo
     * @param transaction_id id of transaction performing write
     */
    template<typename T>
    void store(T *address, T value, uint64_t transaction_id) {
        std::shared_lock<std::shared_mutex> shared_lock(log_mutex_);

        auto &write_buffer = undo_logs_[transaction_id];

        // If we write twice to the same location, we only care about the earliest write
        if (write_buffer.count(address) == 0) {
            void *buffered_val = malloc(sizeof(T));
            std::memcpy(buffered_val, &value, sizeof(T));

            write_buffer.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(reinterpret_cast<void *>(address)),
                    std::forward_as_tuple(buffered_val, sizeof(T)));
        }
    }

    /**
     * Retrieve a reference to the undo log of a transaction
     * @param transaction_id id of transaction
     * @return reference to undo log of transaction
     */
    std::pair<UndoLog &, std::shared_lock<std::shared_mutex>> GetUndoLog(uint64_t transaction_id);


    /**
    * Free all memory related to transaction
    * @param transaction_id id of transaction
    */
    void xend(uint64_t transaction_id);

private:
    std::unordered_map<uint64_t, UndoLog> undo_logs_;
    std::shared_mutex log_mutex_;
};

