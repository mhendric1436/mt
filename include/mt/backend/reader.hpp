#pragma once

#include "mt/collection.hpp"
#include "mt/query.hpp"
#include "mt/types.hpp"

#include <optional>
#include <string_view>

// -----------------------------------------------------------------------------
// mt/backend/reader.hpp
//
// Backend snapshot and metadata read interface.
// -----------------------------------------------------------------------------

namespace mt
{

class IBackendReader
{
  public:
    virtual ~IBackendReader() = default;

    // Returns the latest visible document version at or before version.
    virtual std::optional<DocumentEnvelope> read_snapshot(
        CollectionId collection,
        std::string_view key,
        Version version
    ) = 0;

    // Returns latest committed metadata, including tombstones when present.
    virtual std::optional<DocumentMetadata> read_current_metadata(
        CollectionId collection,
        std::string_view key
    ) = 0;

    // Runs a snapshot query using the backend's advertised query semantics.
    virtual QueryResultEnvelope query_snapshot(
        CollectionId collection,
        const QuerySpec& query,
        Version version
    ) = 0;

    // Runs the same query against latest metadata for conflict validation.
    virtual QueryMetadataResult query_current_metadata(
        CollectionId collection,
        const QuerySpec& query
    ) = 0;

    // Lists snapshot documents using stable key pagination when supported.
    virtual QueryResultEnvelope list_snapshot(
        CollectionId collection,
        const ListOptions& options,
        Version version
    ) = 0;

    // Lists latest metadata using the same list semantics as list_snapshot().
    virtual QueryMetadataResult list_current_metadata(
        CollectionId collection,
        const ListOptions& options
    ) = 0;
};

} // namespace mt
