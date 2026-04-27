#pragma once

#include <stdexcept>

// -----------------------------------------------------------------------------
// mt_errors.hpp
//
// Core exception types.
// -----------------------------------------------------------------------------

namespace mt {

class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class TransactionConflict : public Error {
public:
    using Error::Error;
};

class TransactionClosed : public Error {
public:
    using Error::Error;
};

class DocumentNotFound : public Error {
public:
    using Error::Error;
};

class MappingError : public Error {
public:
    using Error::Error;
};

class BackendError : public Error {
public:
    using Error::Error;
};

} // namespace mt
