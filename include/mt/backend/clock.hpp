#pragma once

#include "mt/collection.hpp"

// -----------------------------------------------------------------------------
// mt/backend/clock.hpp
//
// Backend commit clock interface.
// -----------------------------------------------------------------------------

namespace mt
{

class IBackendClock
{
  public:
    virtual ~IBackendClock() = default;

    // Returns the latest committed version for a new snapshot.
    virtual Version read_clock() = 0;

    // Serializes commits and returns the current committed version.
    virtual Version lock_clock_and_read() = 0;

    // Advances the commit clock. The caller must own the clock lock.
    virtual Version increment_clock_and_return() = 0;
};

} // namespace mt
