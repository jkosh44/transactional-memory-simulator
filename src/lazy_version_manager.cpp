#include "include/lazy_version_manager.h"

void LazyVersionManager::xbegin(uint64_t transaction_id) {
    std::unique_lock<std::shared_mutex> exclusive_lock(buffer_mutex_);
    write_buffers_.emplace(transaction_id, WriteBuffer());
}

std::pair<WriteBuffer &, std::shared_lock<std::shared_mutex>>
LazyVersionManager::GetWriteBuffer(uint64_t transaction_id) {
    std::shared_lock<std::shared_mutex> shared_lock(buffer_mutex_);
    return {write_buffers_.at(transaction_id), std::move(shared_lock)};
}

bool LazyVersionManager::GetValue(uint64_t transaction_id, void *address, void *dest) {
    std::shared_lock<std::shared_mutex> shared_lock(buffer_mutex_);
    if (write_buffers_.count(transaction_id) > 0 && write_buffers_[transaction_id].count(address) > 0) {
        const auto &write = write_buffers_[transaction_id].at(address);
        // TODO this results in a double copy, which may be unavoidable
        std::memcpy(dest, write.data_, write.size_);
        return true;
    }
    return false;
}

void LazyVersionManager::abort(uint64_t transaction_id) {
    // Nothing to do but throw away write buffer
    xend(transaction_id);
}

void LazyVersionManager::xend(uint64_t transaction_id) {
    {
        std::shared_lock<std::shared_mutex> shared_lock(buffer_mutex_);
        auto &write_buffer = write_buffers_[transaction_id];
        for (const auto &write : write_buffer) {
            free(write.second.data_);
        }
    }
    std::unique_lock<std::shared_mutex> exclusive_lock(buffer_mutex_);
    write_buffers_.erase(transaction_id);
}
