#include <iostream>
#include "include/lazy_version_manager.h"

void LazyVersionManager::Store(void *address, void *value, size_t len) {
    void *buffered_val = malloc(len);
    std::memcpy(buffered_val, value, len);

    // If we write twice to the same location, we only care about the most recent write.
    if (write_buffer_.count(address) > 0) {
        auto &write = write_buffer_.at(address);
        free(write.data_);
        write_buffer_.erase(address);
    }

    write_buffer_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(reinterpret_cast<void *>(address)),
            std::forward_as_tuple(buffered_val, len));
}

bool LazyVersionManager::GetValue(void *address, void *dest) {
    if (write_buffer_.count(address) > 0) {
        const auto &write = write_buffer_.at(address);
        // TODO this results in a double copy, which may be unavoidable
        std::memcpy(dest, write.data_, write.size_);
        return true;
    }
    return false;
}

void LazyVersionManager::Abort() {
    for (const auto &write : write_buffer_) {
        free(write.second.data_);
    }
}

void LazyVersionManager::XEnd() {
    for (const auto &write : write_buffer_) {
        std::memcpy(write.first, write.second.data_, write.second.size_);
        free(write.second.data_);
    }
}
