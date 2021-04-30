#pragma once

#include <shared_mutex>
#include <unordered_map>
#include <cstring>

#include "transaction_manager.h"


struct Undo {
    Undo(void *data, size_t size) : data_(data), size_(size) {}

    void *data_;
    size_t size_;
};


using UndoLog = std::unordered_map<void *, Undo>;

class EagerVersionManager {
public:

    /**
     * Store undo into undo buffer
     *
     * @tparam T type of value to undo
     * @param address address to undo
     * @param value value to undo
     */
    template<typename T>
    void Store(T *address, T value) {

        // If we write twice to the same location, we only care about the earliest write
        if (undo_log_.count(address) == 0) {
            void *buffered_val = malloc(sizeof(T));
            std::memcpy(buffered_val, &value, sizeof(T));

            undo_log_.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(reinterpret_cast<void *>(address)),
                    std::forward_as_tuple(buffered_val, sizeof(T)));
        }
    }

    /**
     * Retrieve a reference to the undo log of a transaction
     * @return reference to undo log of transaction
     */
    UndoLog &GetUndoLog();

    /**
     * Abort transaction
     */
    void Abort();

    /**
    * Free all memory related to transaction
    */
    void XEnd();

private:
    UndoLog undo_log_;
};

