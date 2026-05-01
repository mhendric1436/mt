#pragma once

#include "mt/collection.hpp"
#include "mt/types.hpp"

// -----------------------------------------------------------------------------
// mt/backend/writer.hpp
//
// Backend write staging interface.
// -----------------------------------------------------------------------------

namespace mt
{

class IBackendWriter
{
  public:
    virtual ~IBackendWriter() = default;

    // Stages a committed document version or tombstone in history.
    virtual void insert_history(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) = 0;

    // Stages latest current metadata/state for the same commit version.
    virtual void upsert_current(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) = 0;
};

} // namespace mt
