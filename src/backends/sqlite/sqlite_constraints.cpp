#include "sqlite_constraints.hpp"

#include "sqlite_document.hpp"
#include "sqlite_schema.hpp"

#include "mt/errors.hpp"
#include "mt/query.hpp"
#include "mt/schema.hpp"

#include <utility>
#include <vector>

namespace mt::backends::sqlite
{

namespace
{

const IndexSpec* find_index_by_name(
    const std::vector<IndexSpec>& indexes,
    const std::string& name
)
{
    for (const auto& index : indexes)
    {
        if (index.name == name)
        {
            return &index;
        }
    }
    return nullptr;
}

bool same_index_definition(
    const IndexSpec& left,
    const IndexSpec& right
)
{
    return left.name == right.name && left.json_path == right.json_path &&
           left.unique == right.unique;
}

struct UniqueIndexEntry
{
    const IndexSpec* index = nullptr;
    std::string encoded_value;
};

std::string unique_index_value(
    const CollectionSpec& spec,
    const IndexSpec& index,
    const Json& value
)
{
    const auto* field = index_field(spec, index);
    const auto* indexed_value = indexed_json_value(value, index);
    if (!indexed_value || indexed_value->is_null())
    {
        throw BackendError("sqlite backend unique index value must not be null or missing");
    }

    auto encoded = encode_index_scalar_value(*field, *indexed_value);
    if (!encoded)
    {
        throw BackendError("sqlite backend unique index value has incompatible type");
    }
    return *std::move(encoded);
}

std::vector<UniqueIndexEntry> unique_index_entries_for_document(
    const CollectionSpec& spec,
    const Json& value
)
{
    std::vector<UniqueIndexEntry> entries;
    for (const auto& index : spec.indexes)
    {
        if (!index.unique)
        {
            continue;
        }

        entries.push_back(
            UniqueIndexEntry{
                .index = &index, .encoded_value = unique_index_value(spec, index, value)
            }
        );
    }
    return entries;
}

void delete_unique_index_values_for_key(
    detail::Connection& connection,
    CollectionId collection,
    const std::string& key
)
{
    detail::Statement statement{
        connection.get(), detail::PrivateSchemaSql::delete_unique_index_values_for_key()
    };
    statement.bind_int64(1, collection);
    statement.bind_text(2, key);
    statement.step();
}

void insert_unique_index_value(
    detail::Connection& connection,
    CollectionId collection,
    const IndexSpec& index,
    std::string encoded_value,
    const std::string& key
)
{
    detail::Statement statement{
        connection.get(), detail::PrivateSchemaSql::insert_unique_index_value()
    };
    statement.bind_int64(1, collection);
    statement.bind_text(2, index.name);
    statement.bind_text(3, encoded_value);
    statement.bind_text(4, key);
    statement.step();
}

void reject_unique_index_conflict(
    detail::Connection& connection,
    CollectionId collection,
    const IndexSpec& index,
    const std::string& encoded_value,
    const std::string& key
)
{
    detail::Statement statement{
        connection.get(), detail::PrivateSchemaSql::select_unique_index_conflict()
    };
    statement.bind_int64(1, collection);
    statement.bind_text(2, index.name);
    statement.bind_text(3, encoded_value);
    statement.bind_text(4, key);
    if (statement.step())
    {
        throw BackendError("sqlite backend unique index constraint violation");
    }
}

} // namespace

void check_unique_constraints(
    detail::Connection& connection,
    CollectionId collection,
    const WriteEnvelope& write
)
{
    maintain_unique_index_values(connection, collection, write);
}

void maintain_unique_index_values(
    detail::Connection& connection,
    CollectionId collection,
    const WriteEnvelope& write
)
{
    if (write.kind == WriteKind::Delete)
    {
        delete_unique_index_values_for_key(connection, collection, write.key);
        return;
    }

    auto spec = load_collection_spec(connection, collection);
    auto entries = unique_index_entries_for_document(spec, write.value);
    for (const auto& entry : entries)
    {
        reject_unique_index_conflict(
            connection, collection, *entry.index, entry.encoded_value, write.key
        );
    }

    delete_unique_index_values_for_key(connection, collection, write.key);
    for (const auto& entry : entries)
    {
        insert_unique_index_value(
            connection, collection, *entry.index, entry.encoded_value, write.key
        );
    }
}

void validate_sqlite_index_update(
    const CollectionSpec& existing,
    const CollectionSpec& requested
)
{
    for (const auto& existing_index : existing.indexes)
    {
        auto requested_index = find_index_by_name(requested.indexes, existing_index.name);
        if (!requested_index)
        {
            throw BackendError("sqlite backend does not support removing indexes");
        }
        if (!same_index_definition(existing_index, *requested_index))
        {
            throw BackendError("sqlite backend does not support changing index definitions");
        }
    }
}

void rebuild_added_unique_indexes(
    detail::Connection& connection,
    CollectionId collection,
    const CollectionSpec& existing,
    const CollectionSpec& requested
)
{
    std::vector<std::pair<std::string, Json>> documents;
    detail::Statement rows{
        connection.get(), detail::PrivateSchemaSql::select_current_documents_for_index_rebuild()
    };
    rows.bind_int64(1, collection);
    while (rows.step())
    {
        documents.emplace_back(rows.column_text(0), parse_stored_json(rows.column_text(1)));
    }

    for (const auto& [key, value] : documents)
    {
        for (const auto& index : requested.indexes)
        {
            if (!index.unique || find_index_by_name(existing.indexes, index.name))
            {
                continue;
            }

            insert_unique_index_value(
                connection, collection, index, unique_index_value(requested, index, value), key
            );
        }
    }
}

} // namespace mt::backends::sqlite
