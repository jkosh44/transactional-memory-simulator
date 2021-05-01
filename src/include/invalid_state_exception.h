#pragma once

class InvalidStateException : public std::runtime_error {
public:
    explicit InvalidStateException(const char *msg) : std::runtime_error(msg) {}
};