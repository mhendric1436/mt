#include "sqlite_schema.hpp"

#include "../common/schema_codec.hpp"

#include <string>
#include <utility>

namespace mt::backends::sqlite
{

std::optional<CollectionSpec> load_collection_spec(
    detail::Connection& connection,
    std::string_view logical_name
)
{
    detail::Statement statement{
        connection.get(), detail::PrivateSchemaSql::select_collection_spec_by_logical_name()
    };
    statement.bind_text(1, logical_name);
    if (!statement.step())
    {
        return std::nullopt;
    }

    auto schema_json = statement.column_text(3);
    auto indexes_json = statement.column_text(4);
    return CollectionSpec{
        .logical_name = statement.column_text(0),
        .indexes = common::deserialize_indexes(indexes_json),
        .schema_version = static_cast<int>(statement.column_int64(1)),
        .key_field = statement.column_text(2),
        .fields = common::deserialize_fields(schema_json)
    };
}

CollectionDescriptor load_collection_descriptor(
    detail::Connection& connection,
    std::string_view logical_name
)
{
    detail::Statement statement{
        connection.get(), detail::PrivateSchemaSql::select_collection_descriptor_by_logical_name()
    };
    statement.bind_text(1, logical_name);
    if (!statement.step())
    {
        throw BackendError("sqlite collection not found");
    }

    return CollectionDescriptor{
        .id = static_cast<CollectionId>(statement.column_int64(0)),
        .logical_name = statement.column_text(1),
        .schema_version = static_cast<int>(statement.column_int64(2))
    };
}

void insert_collection(
    detail::Connection& connection,
    const CollectionSpec& spec
)
{
    detail::Statement statement{connection.get(), detail::PrivateSchemaSql::insert_collection()};
    statement.bind_text(1, spec.logical_name);
    statement.bind_int64(2, spec.schema_version);
    statement.bind_text(3, spec.key_field);
    statement.bind_text(4, common::serialize_fields(spec.fields));
    statement.bind_text(5, common::serialize_indexes(spec.indexes));
    statement.step();
}

void update_collection(
    detail::Connection& connection,
    const CollectionSpec& spec
)
{
    detail::Statement statement{connection.get(), detail::PrivateSchemaSql::update_collection()};
    statement.bind_int64(1, spec.schema_version);
    statement.bind_text(2, spec.key_field);
    statement.bind_text(3, common::serialize_fields(spec.fields));
    statement.bind_text(4, common::serialize_indexes(spec.indexes));
    statement.bind_text(5, spec.logical_name);
    statement.step();
}

std::vector<IndexSpec> load_collection_indexes(
    detail::Connection& connection,
    CollectionId collection
)
{
    detail::Statement statement{
        connection.get(), detail::PrivateSchemaSql::select_collection_indexes_by_id()
    };
    statement.bind_int64(1, collection);
    if (!statement.step())
    {
        throw BackendError("sqlite collection not found");
    }
    return common::deserialize_indexes(statement.column_text(0));
}

} // namespace mt::backends::sqlite
