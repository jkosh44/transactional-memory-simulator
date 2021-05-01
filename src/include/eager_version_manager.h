#pragma once

#include <shared_mutex>
#include <unordered_map>
#include <cstring>

#include "transaction_manager.h"
#include "version_manager.h"


struct Undo {
    Undo(void *data, size_t size) : data_(data), size_(size) {}

    void *data_;
    size_t size_;
};


using UndoLog = std::unordered_map<void *, Undo>;

class EagerVersionManager : public VersionManager {
public:

    /**
    * Store undo into undo buffer
    *
    * @param address address to undo
    * @param value value to undo
    * @param len length of value
    */
    void Store(void *address, void *value, size_t len) override;

    /**
     * Always returns false
     * @param address
     * @param dest
     * @return false
     */
    bool GetValue(void *address, void *dest);

    /**
     * AbortWithoutLocks transaction
     */
    void Abort() override;

    /**
    * Free all memory related to transaction
    */
    void XEnd() override;

private:
    UndoLog undo_log_;
};

