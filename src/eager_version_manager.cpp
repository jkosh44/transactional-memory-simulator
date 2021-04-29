#include "include/eager_version_manager.h"


UndoLog &EagerVersionManager::GetUndoLog() {
    return undo_log_;
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
