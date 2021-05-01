#include "include/eager_version_manager.h"


void EagerVersionManager::Store(void *address, void *value, size_t len) {
    // If we write twice to the same location, we only care about the earliest write
    if (undo_log_.count(address) == 0) {
        void *buffered_val = malloc(len);
        std::memcpy(buffered_val, address, len);

        undo_log_.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(reinterpret_cast<void *>(address)),
                std::forward_as_tuple(buffered_val, len));
    }
    std::memcpy(address, value, len);
}

bool EagerVersionManager::GetValue(void *address, void *dest) {
    return false;
}

void EagerVersionManager::Abort() {
    for (const auto &undo : undo_log_) {
        std::memcpy(undo.first, undo.second.data_, undo.second.size_);
        free(undo.second.data_);
    }
}

void EagerVersionManager::XEnd() {
    for (const auto &undo : undo_log_) {
        free(undo.second.data_);
    }

}
