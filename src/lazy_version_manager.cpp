#include "include/lazy_version_manager.h"

WriteBuffer &LazyVersionManager::GetWriteBuffer() {
    return write_buffer_;
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
    // Nothing to do but throw away write buffer
    XEnd();
}

void LazyVersionManager::XEnd() {
    for (const auto &write : write_buffer_) {
        free(write.second.data_);
    }
}
