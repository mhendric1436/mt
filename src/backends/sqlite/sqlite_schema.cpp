#include "sqlite_schema.hpp"

#include "mt/errors.hpp"
#include "mt/json.hpp"

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>

namespace mt::backends::sqlite
{

namespace
{
int field_type_to_int(FieldType type)
{
    return static_cast<int>(type);
}

FieldType field_type_from_int(int value)
{
    switch (static_cast<FieldType>(value))
    {
    case FieldType::String:
    case FieldType::Bool:
    case FieldType::Int64:
    case FieldType::Double:
    case FieldType::Optional:
    case FieldType::Array:
    case FieldType::Object:
        return static_cast<FieldType>(value);
    }

    throw BackendError("invalid stored SQLite field type");
}

std::string default_payload(const Json& value)
{
    if (value.is_null())
    {
        return "null";
    }
    if (value.is_bool())
    {
        return value.as_bool() ? "true" : "false";
    }
    if (value.is_int64())
    {
        return std::to_string(value.as_int64());
    }
    if (value.is_double())
    {
        std::ostringstream stream;
        stream << std::setprecision(17) << value.as_double();
        return stream.str();
    }
    if (value.is_string())
    {
        return value.as_string();
    }

    throw BackendError("SQLite schema snapshots support scalar default values only");
}

char default_kind(const Json& value)
{
    if (value.is_null())
    {
        return 'z';
    }
    if (value.is_bool())
    {
        return 'b';
    }
    if (value.is_int64())
    {
        return 'i';
    }
    if (value.is_double())
    {
        return 'd';
    }
    if (value.is_string())
    {
        return 's';
    }

    throw BackendError("SQLite schema snapshots support scalar default values only");
}

Json default_json(
    char kind,
    const std::string& payload
)
{
    switch (kind)
    {
    case 'z':
        return Json::null();
    case 'b':
        return Json(payload == "true");
    case 'i':
        return Json(static_cast<std::int64_t>(std::stoll(payload)));
    case 'd':
        return Json(std::stod(payload));
    case 's':
        return Json(payload);
    }

    throw BackendError("invalid stored SQLite default value kind");
}

void append_field(
    std::ostream& out,
    const FieldSpec& field
)
{
    auto kind = field.has_default ? default_kind(field.default_value) : '-';
    auto payload = field.has_default ? default_payload(field.default_value) : std::string{};

    out << "field " << std::quoted(field.name) << ' ' << field_type_to_int(field.type) << ' '
        << (field.required ? 1 : 0) << ' ' << (field.has_default ? 1 : 0) << ' '
        << field_type_to_int(field.value_type) << ' ' << kind << ' ' << std::quoted(payload) << ' '
        << field.fields.size() << '\n';

    for (const auto& child : field.fields)
    {
        append_field(out, child);
    }
}

FieldSpec read_field(std::istream& in)
{
    std::string marker;
    std::string name;
    int type = 0;
    int required = 0;
    int has_default = 0;
    int value_type = 0;
    char kind = '-';
    std::string payload;
    std::size_t child_count = 0;

    if (!(in >> marker >> std::quoted(name) >> type >> required >> has_default >> value_type >>
          kind >> std::quoted(payload) >> child_count) ||
        marker != "field")
    {
        throw BackendError("invalid stored SQLite field metadata");
    }

    FieldSpec field{
        .name = std::move(name),
        .type = field_type_from_int(type),
        .required = required != 0,
        .has_default = has_default != 0,
        .value_type = field_type_from_int(value_type)
    };
    if (field.has_default)
    {
        field.default_value = default_json(kind, payload);
    }

    field.fields.reserve(child_count);
    for (std::size_t i = 0; i < child_count; ++i)
    {
        field.fields.push_back(read_field(in));
    }

    return field;
}

std::string serialize_fields(const std::vector<FieldSpec>& fields)
{
    std::ostringstream out;
    out << "fields " << fields.size() << '\n';
    for (const auto& field : fields)
    {
        append_field(out, field);
    }
    return out.str();
}

std::vector<FieldSpec> deserialize_fields(const std::string& encoded)
{
    std::istringstream in(encoded);
    std::string marker;
    std::size_t count = 0;
    if (!(in >> marker >> count) || marker != "fields")
    {
        throw BackendError("invalid stored SQLite schema metadata");
    }

    std::vector<FieldSpec> fields;
    fields.reserve(count);
    for (std::size_t i = 0; i < count; ++i)
    {
        fields.push_back(read_field(in));
    }
    return fields;
}

std::string serialize_indexes(const std::vector<IndexSpec>& indexes)
{
    std::ostringstream out;
    out << "indexes " << indexes.size() << '\n';
    for (const auto& index : indexes)
    {
        out << "index " << std::quoted(index.name) << ' ' << std::quoted(index.json_path) << ' '
            << (index.unique ? 1 : 0) << '\n';
    }
    return out.str();
}

std::vector<IndexSpec> deserialize_indexes(const std::string& encoded)
{
    std::istringstream in(encoded);
    std::string marker;
    std::size_t count = 0;
    if (!(in >> marker >> count) || marker != "indexes")
    {
        throw BackendError("invalid stored SQLite index metadata");
    }

    std::vector<IndexSpec> indexes;
    indexes.reserve(count);
    for (std::size_t i = 0; i < count; ++i)
    {
        std::string item_marker;
        IndexSpec index;
        int unique = 0;
        if (!(in >> item_marker >> std::quoted(index.name) >> std::quoted(index.json_path) >>
              unique) ||
            item_marker != "index")
        {
            throw BackendError("invalid stored SQLite index metadata");
        }
        index.unique = unique != 0;
        indexes.push_back(std::move(index));
    }

    return indexes;
}

} // namespace

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
        .indexes = deserialize_indexes(indexes_json),
        .schema_version = static_cast<int>(statement.column_int64(1)),
        .key_field = statement.column_text(2),
        .fields = deserialize_fields(schema_json)
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
    statement.bind_text(4, serialize_fields(spec.fields));
    statement.bind_text(5, serialize_indexes(spec.indexes));
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
    statement.bind_text(3, serialize_fields(spec.fields));
    statement.bind_text(4, serialize_indexes(spec.indexes));
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
    return deserialize_indexes(statement.column_text(0));
}

} // namespace mt::backends::sqlite
