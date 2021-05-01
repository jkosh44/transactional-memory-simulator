#pragma once

#include <cstdint>
#include <unordered_map>
#include <list>
#include <iostream>
#include <shared_mutex>
#include <cstring>

#include "transaction_manager.h"
#include "version_manager.h"

struct Write {
    Write(void *data, size_t size) : data_(data), size_(size) {}

    void *data_;
    size_t size_;
};

using WriteBuffer = std::unordered_map<void *, Write>;

class LazyVersionManager : public VersionManager {

public:

    /**
     * Store write into write buffer
     *
     * @param address address that the write is going to take place on
     * @param value value to write
     * @param len size of value
     */
    void Store(void *address, void *value, size_t len);

    /**
     * Get the value at an address, if there is a buffered write at that address
     * @param transaction_id id of transaction
     * @param address address to get value from
     * @param dest memory location to write value to
     * @return true if value was written to dest, false otherwise
     */
    bool GetValue(void *address, void *dest) override;

    /**
     * Aborts transaction
     */
    void Abort() override;

    /**
     * Free all memory related to transaction
     */
    void XEnd() override;

private:
    WriteBuffer write_buffer_;
};
