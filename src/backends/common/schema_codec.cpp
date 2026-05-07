#include "schema_codec.hpp"

#include "mt/errors.hpp"
#include "mt/json.hpp"
#include "mt/json_parser.hpp"

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>

namespace mt::backends::common
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
    case FieldType::Json:
    case FieldType::Optional:
    case FieldType::Array:
    case FieldType::Object:
        return static_cast<FieldType>(value);
    }

    throw BackendError("invalid stored field type");
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

    return value.canonical_string();
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
    if (value.is_array() || value.is_object())
    {
        return 'j';
    }

    throw BackendError("unsupported schema default value");
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
    case 'j':
        return parse_json(payload);
    }

    throw BackendError("invalid stored default value kind");
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
        throw BackendError("invalid stored field metadata");
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

} // namespace

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

std::vector<FieldSpec> deserialize_fields(std::string_view encoded)
{
    std::istringstream in{std::string(encoded)};
    std::string marker;
    std::size_t count = 0;
    if (!(in >> marker >> count) || marker != "fields")
    {
        throw BackendError("invalid stored schema metadata");
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

std::vector<IndexSpec> deserialize_indexes(std::string_view encoded)
{
    std::istringstream in{std::string(encoded)};
    std::string marker;
    std::size_t count = 0;
    if (!(in >> marker >> count) || marker != "indexes")
    {
        throw BackendError("invalid stored index metadata");
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
            throw BackendError("invalid stored index metadata");
        }
        index.unique = unique != 0;
        indexes.push_back(std::move(index));
    }

    return indexes;
}

} // namespace mt::backends::common
