#pragma once

#include "mt/json.hpp"
#include "mt/query.hpp"

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------
// mt/collection.hpp
//
// Collection descriptors and migration specs.
// -----------------------------------------------------------------------------

namespace mt
{

using CollectionId = std::uint64_t;
using Version = std::uint64_t;
using TxId = std::string;

enum class FieldType
{
    String,
    Bool,
    Int64,
    Double,
    Json,
    Optional,
    Array,
    Object
};

struct FieldSpec
{
    std::string name;
    FieldType type = FieldType::String;
    bool required = true;
    bool has_default = false;
    Json default_value;
    FieldType value_type = FieldType::String;
    std::vector<FieldSpec> fields;

    static FieldSpec string(std::string name)
    {
        return FieldSpec{.name = std::move(name), .type = FieldType::String};
    }

    static FieldSpec boolean(std::string name)
    {
        return FieldSpec{.name = std::move(name), .type = FieldType::Bool};
    }

    static FieldSpec int64(std::string name)
    {
        return FieldSpec{.name = std::move(name), .type = FieldType::Int64};
    }

    static FieldSpec double_value(std::string name)
    {
        return FieldSpec{.name = std::move(name), .type = FieldType::Double};
    }

    static FieldSpec json(std::string name)
    {
        return FieldSpec{.name = std::move(name), .type = FieldType::Json};
    }

    static FieldSpec optional(
        std::string name,
        FieldType value_type
    )
    {
        return FieldSpec{
            .name = std::move(name), .type = FieldType::Optional, .value_type = value_type
        };
    }

    static FieldSpec array(
        std::string name,
        FieldType value_type
    )
    {
        return FieldSpec{
            .name = std::move(name), .type = FieldType::Array, .value_type = value_type
        };
    }

    static FieldSpec array_object(
        std::string name,
        std::vector<FieldSpec> fields
    )
    {
        return FieldSpec{
            .name = std::move(name),
            .type = FieldType::Array,
            .value_type = FieldType::Object,
            .fields = std::move(fields)
        };
    }

    static FieldSpec object(
        std::string name,
        std::vector<FieldSpec> fields
    )
    {
        return FieldSpec{
            .name = std::move(name), .type = FieldType::Object, .fields = std::move(fields)
        };
    }

    FieldSpec mark_required(bool value) const
    {
        auto copy = *this;
        copy.required = value;
        return copy;
    }

    FieldSpec with_default(Json value) const
    {
        auto copy = *this;
        copy.has_default = true;
        copy.default_value = std::move(value);
        return copy;
    }
};

struct Migration
{
    int from_version = 0;
    int to_version = 0;
    std::function<void(Json&)> transform;
};

struct CollectionSpec
{
    std::string logical_name;
    std::vector<IndexSpec> indexes;
    int schema_version = 1;
    std::string key_field;
    std::vector<FieldSpec> fields;
    std::vector<Migration> migrations;
};

struct CollectionDescriptor
{
    CollectionId id = 0;
    std::string logical_name;
    int schema_version = 1;
};

struct BootstrapSpec
{
    int metadata_schema_version = 1;
};

} // namespace mt
