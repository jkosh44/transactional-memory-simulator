#include "include/eager_version_manager.h"


void EagerVersionManager::xbegin(uint64_t transaction_id) {
    std::unique_lock<std::shared_mutex> exclusive_lock(log_mutex_);
    undo_logs_.emplace(transaction_id, UndoLog());
}

std::pair<UndoLog &, std::shared_lock<std::shared_mutex>> EagerVersionManager::GetUndoLog(uint64_t transaction_id) {
    std::shared_lock<std::shared_mutex> shared_lock(log_mutex_);
    return {undo_logs_.at(transaction_id), std::move(shared_lock)};
}

void EagerVersionManager::xend(uint64_t transaction_id) {
    std::unique_lock<std::shared_mutex> exclusive_lock(log_mutex_);
    auto &undo_log = undo_logs_[transaction_id];
    for (const auto &undo : undo_log) {
        free(undo.second.data_);
    }
    undo_logs_.erase(transaction_id);
}
