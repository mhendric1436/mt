#pragma once

#include "mt/collection.hpp"
#include "mt/hash.hpp"
#include "mt/json.hpp"
#include "mt/query.hpp"

#include <string>
#include <vector>

// -----------------------------------------------------------------------------
// mt/types.hpp
//
// Backend document envelopes and write descriptions.
// -----------------------------------------------------------------------------

namespace mt
{

struct DocumentEnvelope
{
    CollectionId collection = 0;
    Key key;
    Version version = 0;
    bool deleted = false;
    Hash value_hash;
    Json value;
};

struct DocumentMetadata
{
    CollectionId collection = 0;
    Key key;
    Version version = 0;
    bool deleted = false;
    Hash value_hash;
};

struct QueryResultEnvelope
{
    std::vector<DocumentEnvelope> rows;
};

struct QueryMetadataResult
{
    std::vector<DocumentMetadata> rows;
};

enum class WriteKind
{
    Put,
    Delete
};

struct WriteEnvelope
{
    CollectionId collection = 0;
    Key key;
    WriteKind kind = WriteKind::Put;
    Json value;
    Hash value_hash;
};

} // namespace mt
