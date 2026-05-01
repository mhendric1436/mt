#pragma once

#include "mt/backend/active_transactions.hpp"
#include "mt/backend/clock.hpp"
#include "mt/backend/lifecycle.hpp"
#include "mt/backend/reader.hpp"
#include "mt/backend/writer.hpp"

// -----------------------------------------------------------------------------
// mt/backend/session.hpp
//
// Backend transaction/session interface used by mt transactions.
// -----------------------------------------------------------------------------

namespace mt
{

class IBackendSession : public IBackendLifecycle,
                        public IBackendClock,
                        public IBackendActiveTransactions,
                        public IBackendReader,
                        public IBackendWriter
{
  public:
    ~IBackendSession() override = default;
};

} // namespace mt
