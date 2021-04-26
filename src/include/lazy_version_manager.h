#pragma once

#include <cstdint>
#include <unordered_map>
#include <list>
#include <shared_mutex>
#include <cstring>

struct Write {
    Write(void *data, size_t size) : data_(data), size_(size) {}

    void *data_;
    size_t size_;
};

using WriteBuffer = std::unordered_map<void *, Write>;

class LazyVersionManager {

public:
    /**
     * Begin transaction with Lazy Version Manager
     *
     * @param transaction_id
     */
    void xbegin(uint64_t transaction_id);

    /**
     * Store write into write buffer
     *
     * @tparam T type of value to write
     * @param address address that the write is going to take place on
     * @param value value to write
     * @param transaction_id id of transaction performing write
     */
    template<typename T>
    void store(T *address, T value, uint64_t transaction_id) {
        std::shared_lock<std::shared_mutex> shared_lock(buffer_mutex_);

        void *buffered_val = malloc(sizeof(T));
        std::memcpy(buffered_val, &value, sizeof(T));

        auto &write_buffer = write_buffers_[transaction_id];

        // If we write twice to the same location, we only care about the most recent write.
        if (write_buffer.count(address) > 0) {
            auto &write = write_buffer.at(address);
            free(write.data_);
            write_buffer.erase(address);
        }

        write_buffer.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(reinterpret_cast<void *>(address)),
                std::forward_as_tuple(buffered_val, sizeof(T)));
    }

    /**
     * Retrieve a reference to the write buffer of a transaction
     * @param transaction_id id of transaction
     * @return reference to write buffer of transaction
     */
    std::pair<WriteBuffer &, std::shared_lock<std::shared_mutex>> GetWriteBuffer(uint64_t transaction_id);

    /**
     * Get the value at an address associated with a specific transaction, if there is a buffered write at that address
     * @param transaction_id id of transaction
     * @param address address to get value from
     * @param dest memory location to write value to
     * @return true if value was written to dest, false otherwise
     */
    bool GetValue(uint64_t transaction_id, void *address, void *dest);

    /**
     * Aborts transaction
     * @param transaction_id id of transaction
     */
    void abort(uint64_t transaction_id);

    /**
     * Free all memory related to transaction
     * @param transaction_id id of transaction
     */
    void xend(uint64_t transaction_id);

private:
    std::unordered_map<uint64_t, WriteBuffer> write_buffers_;
    std::shared_mutex buffer_mutex_;
};
