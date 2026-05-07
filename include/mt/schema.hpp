#pragma once

#include "mt/collection.hpp"

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// -----------------------------------------------------------------------------
// mt/schema.hpp
//
// Pure schema comparison helpers for backend schema evolution.
// -----------------------------------------------------------------------------

namespace mt
{

enum class SchemaChangeKind
{
    AddField,
    RemoveField,
    ChangeKeyField,
    ChangeFieldType,
    ChangeValueType,
    ChangeRequired,
    ChangeDefault
};

struct SchemaChange
{
    SchemaChangeKind kind = SchemaChangeKind::AddField;
    std::string path;
    std::string message;
};

struct SchemaDiff
{
    std::vector<SchemaChange> compatible_changes;
    std::vector<SchemaChange> incompatible_changes;

    bool is_compatible() const noexcept
    {
        return incompatible_changes.empty();
    }

    bool empty() const noexcept
    {
        return compatible_changes.empty() && incompatible_changes.empty();
    }
};

inline std::string field_type_name(FieldType type)
{
    switch (type)
    {
    case FieldType::String:
        return "string";
    case FieldType::Bool:
        return "bool";
    case FieldType::Int64:
        return "int64";
    case FieldType::Double:
        return "double";
    case FieldType::Json:
        return "json";
    case FieldType::Optional:
        return "optional";
    case FieldType::Array:
        return "array";
    case FieldType::Object:
        return "object";
    }

    return "unknown";
}

inline bool field_addition_is_compatible(const FieldSpec& field)
{
    return !field.required || field.has_default || field.type == FieldType::Optional;
}

inline void add_compatible_change(
    SchemaDiff& diff,
    SchemaChangeKind kind,
    std::string path,
    std::string message
)
{
    diff.compatible_changes.push_back(
        SchemaChange{.kind = kind, .path = std::move(path), .message = std::move(message)}
    );
}

inline void add_incompatible_change(
    SchemaDiff& diff,
    SchemaChangeKind kind,
    std::string path,
    std::string message
)
{
    diff.incompatible_changes.push_back(
        SchemaChange{.kind = kind, .path = std::move(path), .message = std::move(message)}
    );
}

inline std::string field_path(
    std::string_view parent_path,
    std::string_view field_name
)
{
    if (parent_path == "$")
    {
        return "$." + std::string(field_name);
    }

    return std::string(parent_path) + "." + std::string(field_name);
}

inline bool defaults_equal(
    const FieldSpec& stored,
    const FieldSpec& requested
)
{
    if (stored.has_default != requested.has_default)
    {
        return false;
    }

    if (!stored.has_default)
    {
        return true;
    }

    return stored.default_value == requested.default_value;
}

inline void diff_fields(
    SchemaDiff& diff,
    const std::vector<FieldSpec>& stored_fields,
    const std::vector<FieldSpec>& requested_fields,
    std::string_view parent_path
)
{
    std::unordered_map<std::string, const FieldSpec*> requested_by_name;
    for (const auto& field : requested_fields)
    {
        requested_by_name.emplace(field.name, &field);
    }

    std::unordered_map<std::string, const FieldSpec*> stored_by_name;
    for (const auto& field : stored_fields)
    {
        stored_by_name.emplace(field.name, &field);
    }

    for (const auto& stored : stored_fields)
    {
        auto path = field_path(parent_path, stored.name);
        auto requested_it = requested_by_name.find(stored.name);
        if (requested_it == requested_by_name.end())
        {
            add_incompatible_change(
                diff, SchemaChangeKind::RemoveField, path, "field was removed from requested schema"
            );
            continue;
        }

        const auto& requested = *requested_it->second;
        if (stored.type != requested.type)
        {
            add_incompatible_change(
                diff, SchemaChangeKind::ChangeFieldType, path,
                "field type changed from " + field_type_name(stored.type) + " to " +
                    field_type_name(requested.type)
            );
            continue;
        }

        if ((stored.type == FieldType::Optional || stored.type == FieldType::Array) &&
            stored.value_type != requested.value_type)
        {
            add_incompatible_change(
                diff, SchemaChangeKind::ChangeValueType, path,
                "field value type changed from " + field_type_name(stored.value_type) + " to " +
                    field_type_name(requested.value_type)
            );
        }

        if (!defaults_equal(stored, requested))
        {
            add_incompatible_change(
                diff, SchemaChangeKind::ChangeDefault, path, "field default changed"
            );
        }

        if (!stored.required && requested.required)
        {
            add_incompatible_change(
                diff, SchemaChangeKind::ChangeRequired, path, "field became required"
            );
        }
        else if (stored.required && !requested.required)
        {
            add_compatible_change(
                diff, SchemaChangeKind::ChangeRequired, path, "field became non-required"
            );
        }

        if (stored.type == FieldType::Object ||
            (stored.type == FieldType::Array && stored.value_type == FieldType::Object))
        {
            diff_fields(diff, stored.fields, requested.fields, path);
        }
    }

    for (const auto& requested : requested_fields)
    {
        if (stored_by_name.contains(requested.name))
        {
            continue;
        }

        auto path = field_path(parent_path, requested.name);
        if (field_addition_is_compatible(requested))
        {
            add_compatible_change(
                diff, SchemaChangeKind::AddField, path, "field was added compatibly"
            );
        }
        else
        {
            add_incompatible_change(
                diff, SchemaChangeKind::AddField, path, "required field was added without a default"
            );
        }
    }
}

inline SchemaDiff diff_schemas(
    const CollectionSpec& stored,
    const CollectionSpec& requested
)
{
    SchemaDiff diff;

    if (stored.key_field != requested.key_field)
    {
        add_incompatible_change(
            diff, SchemaChangeKind::ChangeKeyField, "$",
            "key field changed from '" + stored.key_field + "' to '" + requested.key_field + "'"
        );
    }

    diff_fields(diff, stored.fields, requested.fields, "$");

    return diff;
}

} // namespace mt
