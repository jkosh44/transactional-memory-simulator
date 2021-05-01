#pragma once


class VersionManager {
public:
    virtual ~VersionManager() = default;

    virtual void Store(void *address, void *value, size_t len) = 0;

    virtual bool GetValue(void *address, void *dest) = 0;

    virtual void Abort() = 0;

    virtual void XEnd() = 0;
};


