#pragma once

#include <cstdint>
#include <unordered_map>
#include <list>
#include <shared_mutex>
#include <cstring>

#include "transaction_manager.h"

struct Write {
    Write(void *data, size_t size) : data_(data), size_(size) {}

    void *data_;
    size_t size_;
};

using WriteBuffer = std::unordered_map<void *, Write>;

class LazyVersionManager {

public:

    /**
     * Store write into write buffer
     *
     * @tparam T type of value to write
     * @param address address that the write is going to take place on
     * @param value value to write
     */
    template<typename T>
    void Store(T *address, T value) {
        void *buffered_val = malloc(sizeof(T));
        std::memcpy(buffered_val, &value, sizeof(T));

        // If we write twice to the same location, we only care about the most recent write.
        if (write_buffer_.count(address) > 0) {
            auto &write = write_buffer_.at(address);
            free(write.data_);
            write_buffer_.erase(address);
        }

        write_buffer_.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(reinterpret_cast<void *>(address)),
                std::forward_as_tuple(buffered_val, sizeof(T)));
    }

    /**
     * Retrieve a reference to the write buffer of a transaction
     * @return reference to write buffer of transaction
     */
    WriteBuffer &GetWriteBuffer();

    /**
     * Get the value at an address, if there is a buffered write at that address
     * @param transaction_id id of transaction
     * @param address address to get value from
     * @param dest memory location to write value to
     * @return true if value was written to dest, false otherwise
     */
    bool GetValue(void *address, void *dest);

    /**
     * Aborts transaction
     */
    void Abort();

    /**
     * Free all memory related to transaction
     */
    void XEnd();

private:
    WriteBuffer write_buffer_;
};
