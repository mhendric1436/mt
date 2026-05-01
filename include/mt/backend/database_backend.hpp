#pragma once

#include "mt/backend/capabilities.hpp"
#include "mt/backend/session.hpp"
#include "mt/collection.hpp"

#include <memory>
#include <string_view>

// -----------------------------------------------------------------------------
// mt/backend/database_backend.hpp
//
// Public backend provider interface for concrete storage engines.
// -----------------------------------------------------------------------------

namespace mt
{

class IDatabaseBackend
{
  public:
    virtual ~IDatabaseBackend() = default;

    // Must truthfully describe query and schema features implemented by this backend.
    virtual BackendCapabilities capabilities() const = 0;

    // Opens a session for one logical backend transaction.
    virtual std::unique_ptr<IBackendSession> open_session() = 0;

    // Initializes backend metadata storage. Must be safe to call more than once.
    virtual void bootstrap(const BootstrapSpec& spec) = 0;

    // Creates or returns a collection descriptor matching the requested spec.
    virtual CollectionDescriptor ensure_collection(const CollectionSpec& spec) = 0;

    // Returns an existing collection descriptor by logical name.
    virtual CollectionDescriptor get_collection(std::string_view logical_name) = 0;
};

} // namespace mt
