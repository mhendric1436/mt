#pragma once

// -----------------------------------------------------------------------------
// mt/backend/lifecycle.hpp
//
// Backend transaction lifecycle interface.
// -----------------------------------------------------------------------------

namespace mt
{

class IBackendLifecycle
{
  public:
    virtual ~IBackendLifecycle() = default;

    // Starts the backend transaction that bounds all session operations.
    virtual void begin_backend_transaction() = 0;

    // Makes all staged mt mutations visible atomically.
    virtual void commit_backend_transaction() = 0;

    // Aborts/closes the backend transaction and releases resources. This is
    // cleanup, not logical undo of committed mt changes.
    virtual void abort_backend_transaction() noexcept = 0;
};

} // namespace mt
