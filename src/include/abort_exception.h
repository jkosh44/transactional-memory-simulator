#pragma once

class AbortException : public std::runtime_error {
public:
    explicit AbortException(const char *msg) : std::runtime_error(msg) {}
};