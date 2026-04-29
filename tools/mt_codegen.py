#!/usr/bin/env python3

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path


IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SUPPORTED_TYPES = {"string", "bool", "int64", "double"}


class SchemaError(Exception):
    pass


@dataclass(frozen=True)
class ScalarType:
    name: str


@dataclass(frozen=True)
class OptionalType:
    inner: ScalarType


@dataclass(frozen=True)
class ArrayType:
    inner: ScalarType


@dataclass(frozen=True)
class ObjectType:
    class_name: str
    fields: tuple


def fail(message):
    raise SchemaError(message)


def require_object(value, context):
    if not isinstance(value, dict):
        fail(f"{context} must be an object")
    return value


def require_present(schema, key):
    if key not in schema:
        fail(f"missing required field: {key}")
    return schema[key]


def require_string(schema, key):
    value = require_present(schema, key)
    if not isinstance(value, str) or not value:
        fail(f"{key} must be a non-empty string")
    return value


def require_named_string(schema, key, context):
    value = schema.get(key)
    if not isinstance(value, str) or not value:
        fail(f"{context} must be a non-empty string")
    return value


def validate_identifier(value, context):
    if not IDENT_RE.match(value):
        fail(f"{context} must be a valid C++ identifier: {value!r}")


def validate_namespace(value):
    if not value:
        return
    for segment in value.split("::"):
        validate_identifier(segment, "namespace segment")


def validate_index_path(path, field_names, context):
    if path == "$":
        fail(f"{context} must reference a declared field")
    if path.rfind("$.", 0) != 0:
        fail(f"{context} must start with '$.'")

    segments = path[2:].split(".")
    if len(segments) != 1 or not segments[0]:
        fail(f"{context} must reference a top-level generated field")

    field_name = segments[0]
    if field_name not in field_names:
        fail(f"{context} references unknown field {field_name!r}")


def parse_scalar_type(type_name, context):
    if type_name not in SUPPORTED_TYPES:
        fail(f"unsupported type for {context}: {type_name!r}")
    return ScalarType(type_name)


def parse_field_type(field, context):
    type_name = require_named_string(field, "type", f"{context}.type")
    if type_name == "optional":
        value_type = require_named_string(field, "value_type", f"{context}.value_type")
        return OptionalType(parse_scalar_type(value_type, f"{context}.value_type"))
    if type_name == "array":
        value_type = require_named_string(field, "value_type", f"{context}.value_type")
        return ArrayType(parse_scalar_type(value_type, f"{context}.value_type"))
    if type_name == "object":
        class_name = require_named_string(field, "class_name", f"{context}.class_name")
        validate_identifier(class_name, f"{context}.class_name")
        fields = parse_fields(field.get("fields"), f"{context}.fields")
        return ObjectType(class_name, tuple(fields))

    return parse_scalar_type(type_name, context)


def cpp_string(value):
    return json.dumps(value)


def cpp_default_value(type_desc, value, context):
    if isinstance(type_desc, ScalarType):
        if type_desc.name == "string":
            if not isinstance(value, str):
                fail(f"default for field {context!r} must be a string")
            return f" = {cpp_string(value)}"

        if type_desc.name == "bool":
            if not isinstance(value, bool):
                fail(f"default for field {context!r} must be a bool")
            return " = true" if value else " = false"

        if type_desc.name == "int64":
            if not isinstance(value, int) or isinstance(value, bool):
                fail(f"default for field {context!r} must be an integer")
            return f" = {value}"

        if type_desc.name == "double":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                fail(f"default for field {context!r} must be numeric")
            return f" = {value}"

    fail(f"unsupported field type descriptor for {context!r}")


def cpp_default(field):
    if not field.get("has_default", False):
        return ""

    return cpp_default_value(field["type"], field["default"], field["name"])


def cpp_field_type(type_desc):
    if isinstance(type_desc, ScalarType):
        if type_desc.name == "string":
            return "mt::FieldType::String"
        if type_desc.name == "bool":
            return "mt::FieldType::Bool"
        if type_desc.name == "int64":
            return "mt::FieldType::Int64"
        if type_desc.name == "double":
            return "mt::FieldType::Double"
    fail(f"unsupported field type descriptor: {type_desc!r}")


def cpp_json_value(type_desc, value, context):
    if isinstance(type_desc, ScalarType):
        if type_desc.name == "string":
            if not isinstance(value, str):
                fail(f"default for field {context!r} must be a string")
            return f"mt::Json({cpp_string(value)})"

        if type_desc.name == "bool":
            if not isinstance(value, bool):
                fail(f"default for field {context!r} must be a bool")
            return "mt::Json(true)" if value else "mt::Json(false)"

        if type_desc.name == "int64":
            if not isinstance(value, int) or isinstance(value, bool):
                fail(f"default for field {context!r} must be an integer")
            return f"mt::Json(std::int64_t{{{value}}})"

        if type_desc.name == "double":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                fail(f"default for field {context!r} must be numeric")
            return f"mt::Json({value})"

    fail(f"unsupported field type descriptor for {context!r}")


def cpp_field_spec(field):
    type_desc = field["type"]
    name = cpp_string(field["name"])

    if isinstance(type_desc, ScalarType):
        if type_desc.name == "string":
            expr = f"mt::FieldSpec::string({name})"
        elif type_desc.name == "bool":
            expr = f"mt::FieldSpec::boolean({name})"
        elif type_desc.name == "int64":
            expr = f"mt::FieldSpec::int64({name})"
        elif type_desc.name == "double":
            expr = f"mt::FieldSpec::double_value({name})"
        else:
            fail(f"unsupported field type descriptor: {type_desc!r}")
    elif isinstance(type_desc, OptionalType):
        expr = f"mt::FieldSpec::optional({name}, {cpp_field_type(type_desc.inner)})"
    elif isinstance(type_desc, ArrayType):
        expr = f"mt::FieldSpec::array({name}, {cpp_field_type(type_desc.inner)})"
    elif isinstance(type_desc, ObjectType):
        nested = ", ".join(cpp_field_spec(nested_field) for nested_field in type_desc.fields)
        expr = f"mt::FieldSpec::object({name}, {{{nested}}})"
    else:
        fail(f"unsupported field type descriptor: {type_desc!r}")

    expr += f".mark_required({'true' if field['required'] else 'false'})"
    if field.get("has_default", False):
        expr += f".with_default({cpp_json_value(type_desc, field['default'], field['name'])})"
    return expr


def cpp_type(type_desc):
    if isinstance(type_desc, ScalarType):
        if type_desc.name == "string":
            return "std::string"
        if type_desc.name == "bool":
            return "bool"
        if type_desc.name == "int64":
            return "std::int64_t"
        if type_desc.name == "double":
            return "double"
    if isinstance(type_desc, OptionalType):
        return f"std::optional<{cpp_type(type_desc.inner)}>"
    if isinstance(type_desc, ArrayType):
        return f"std::vector<{cpp_type(type_desc.inner)}>"
    if isinstance(type_desc, ObjectType):
        return type_desc.class_name
    fail(f"unsupported field type descriptor: {type_desc!r}")


def json_accessor(type_desc):
    if isinstance(type_desc, ScalarType):
        if type_desc.name == "string":
            return "as_string()"
        if type_desc.name == "bool":
            return "as_bool()"
        if type_desc.name == "int64":
            return "as_int64()"
        if type_desc.name == "double":
            return "as_double()"
    fail(f"unsupported field type descriptor: {type_desc!r}")


def cpp_to_json_expr(type_desc, value_expr):
    if isinstance(type_desc, ScalarType):
        return value_expr
    if isinstance(type_desc, OptionalType):
        return f"{value_expr} ? mt::Json(*{value_expr}) : mt::Json::null()"
    if isinstance(type_desc, ArrayType):
        return f"to_json_array({value_expr})"
    if isinstance(type_desc, ObjectType):
        return f"to_json({value_expr})"
    fail(f"unsupported field type descriptor: {type_desc!r}")


def json_to_cpp_expr(type_desc, json_expr):
    if isinstance(type_desc, ScalarType):
        return f"{json_expr}.{json_accessor(type_desc)}"
    if isinstance(type_desc, OptionalType):
        inner_expr = json_to_cpp_expr(type_desc.inner, json_expr)
        return f"{json_expr}.is_null() ? std::nullopt : std::optional<{cpp_type(type_desc.inner)}>({inner_expr})"
    if isinstance(type_desc, ArrayType):
        return f"from_json_array_{type_desc.inner.name}({json_expr})"
    if isinstance(type_desc, ObjectType):
        return f"from_json_{type_desc.class_name}({json_expr})"
    fail(f"unsupported field type descriptor: {type_desc!r}")


def fields_need_optional(fields):
    for field in fields:
        type_desc = field["type"]
        if isinstance(type_desc, OptionalType):
            return True
        if isinstance(type_desc, ObjectType) and fields_need_optional(type_desc.fields):
            return True
    return False


def needs_optional(schema):
    return fields_need_optional(schema["fields"])


def array_inner_types_in_fields(fields):
    return sorted(
        {
            field["type"].inner.name
            for field in fields
            if isinstance(field["type"], ArrayType)
        }.union(
            *[
                set(array_inner_types_in_fields(field["type"].fields))
                for field in fields
                if isinstance(field["type"], ObjectType)
            ]
        )
    )


def array_inner_types(schema):
    return array_inner_types_in_fields(schema["fields"])


def object_types_in_fields(fields):
    objects = []
    for field in fields:
        type_desc = field["type"]
        if isinstance(type_desc, ObjectType):
            objects.extend(object_types_in_fields(type_desc.fields))
            objects.append(type_desc)
    return objects


def object_types(schema):
    unique = []
    seen = set()
    for type_desc in object_types_in_fields(schema["fields"]):
        if type_desc.class_name in seen:
            continue
        seen.add(type_desc.class_name)
        unique.append(type_desc)
    return unique


def parse_fields(fields, context):
    if not isinstance(fields, list) or not fields:
        fail(f"{context} must be a non-empty array")

    seen = set()
    parsed = []
    for index, field in enumerate(fields):
        field = require_object(field, f"{context}[{index}]")
        name = require_named_string(field, "name", f"{context}[{index}].name")
        validate_identifier(name, "field name")
        if name in seen:
            fail(f"duplicate field name {name!r}")
        seen.add(name)

        type_desc = parse_field_type(field, f"{context}[{index}]")

        if "required" in field and not isinstance(field["required"], bool):
            fail(f"required for field {name!r} must be a bool")
        if "default" in field:
            cpp_default_value(type_desc, field["default"], name)

        parsed.append(
            {
                "name": name,
                "type": type_desc,
                "required": field.get("required", "default" not in field),
                "default": field.get("default"),
                "has_default": "default" in field,
            }
        )

    return parsed


def validate_unique_object_class_names(fields):
    seen = {}
    for type_desc in object_types_in_fields(fields):
        existing = seen.get(type_desc.class_name)
        if existing is not None and existing != type_desc.fields:
            fail(f"duplicate object class_name with different fields: {type_desc.class_name!r}")
        seen[type_desc.class_name] = type_desc.fields


def validate_schema(schema):
    schema = require_object(schema, "schema")

    namespace = schema.get("namespace", "")
    if namespace is not None and not isinstance(namespace, str):
        fail("namespace must be a string")
    namespace = namespace or ""
    validate_namespace(namespace)

    table_name = require_string(schema, "table_name")
    class_name = require_string(schema, "class_name")
    key = require_string(schema, "key")
    validate_identifier(class_name, "class_name")

    schema_version = schema.get("schema_version", 1)
    if not isinstance(schema_version, int) or isinstance(schema_version, bool) or schema_version < 1:
        fail("schema_version must be a positive integer")

    validated_fields = parse_fields(schema.get("fields"), "fields")
    validate_unique_object_class_names(validated_fields)

    seen = {field["name"] for field in validated_fields}
    if key not in seen:
        fail(f"key field {key!r} does not exist")

    indexes = schema.get("indexes", [])
    if indexes is None:
        indexes = []
    if not isinstance(indexes, list):
        fail("indexes must be an array")

    validated_indexes = []
    index_names = set()
    for index, item in enumerate(indexes):
        item = require_object(item, f"indexes[{index}]")
        name = require_named_string(item, "name", f"indexes[{index}].name")
        path = require_named_string(item, "path", f"indexes[{index}].path")
        validate_index_path(path, seen, f"indexes[{index}].path")
        if name in index_names:
            fail(f"duplicate index name {name!r}")
        index_names.add(name)
        unique = item.get("unique", False)
        if not isinstance(unique, bool):
            fail(f"unique for index {name!r} must be a bool")
        validated_indexes.append({"name": name, "path": path, "unique": unique})

    return {
        "namespace": namespace,
        "table_name": table_name,
        "class_name": class_name,
        "key": key,
        "schema_version": schema_version,
        "fields": validated_fields,
        "indexes": validated_indexes,
    }


def append_struct(lines, class_name, fields):
    lines.append(f"struct {class_name}")
    lines.append("{")
    for field in fields:
        lines.append(f"    {cpp_type(field['type'])} {field['name']}{cpp_default(field)};")
    lines.append("")
    lines.append(f"    friend bool operator==(const {class_name}&, const {class_name}&) = default;")
    lines.append("};")
    lines.append("")


def append_object_json_helpers(lines, object_type):
    lines.append(f"    static mt::Json to_json(const {object_type.class_name}& row)")
    lines.append("    {")
    lines.append("        return mt::Json::object(")
    lines.append("            {")
    for index, field in enumerate(object_type.fields):
        comma = "," if index + 1 < len(object_type.fields) else ""
        value_expr = cpp_to_json_expr(field["type"], f"row.{field['name']}")
        lines.append(f"                {{{cpp_string(field['name'])}, {value_expr}}}{comma}")
    lines.append("            }")
    lines.append("        );")
    lines.append("    }")
    lines.append("")

    lines.append(f"    static {object_type.class_name} from_json_{object_type.class_name}(const mt::Json& json)")
    lines.append("    {")
    lines.append(f"        return {object_type.class_name}{{")
    for index, field in enumerate(object_type.fields):
        comma = "," if index + 1 < len(object_type.fields) else ""
        json_expr = f"json[{cpp_string(field['name'])}]"
        lines.append(f"            .{field['name']} = {json_to_cpp_expr(field['type'], json_expr)}{comma}")
    lines.append("        };")
    lines.append("    }")


def render(schema):
    class_name = schema["class_name"]
    mapping_name = f"{class_name}Mapping"
    lines = []

    lines.extend(
        [
            "#pragma once",
            "",
            '#include "mt/core.hpp"',
            "",
            "#include <cstdint>",
            "#include <string>",
            "#include <string_view>",
            "#include <vector>",
            "",
        ]
    )
    if needs_optional(schema):
        lines.insert(5, "#include <optional>")

    if schema["namespace"]:
        lines.append(f"namespace {schema['namespace']}")
        lines.append("{")
        lines.append("")

    for type_desc in object_types(schema):
        append_struct(lines, type_desc.class_name, type_desc.fields)

    append_struct(lines, class_name, schema["fields"])

    lines.append(f"struct {mapping_name}")
    lines.append("{")
    lines.append(f"    static constexpr std::string_view table_name = {cpp_string(schema['table_name'])};")
    lines.append(f"    static constexpr int schema_version = {schema['schema_version']};")
    lines.append(f"    static constexpr std::string_view key_field = {cpp_string(schema['key'])};")
    lines.append("")
    lines.append(f"    static std::string key(const {class_name}& row)")
    lines.append("    {")
    lines.append(f"        return row.{schema['key']};")
    lines.append("    }")
    lines.append("")
    lines.append("    static std::vector<mt::FieldSpec> fields()")
    lines.append("    {")
    lines.append("        return {")
    for index, field in enumerate(schema["fields"]):
        comma = "," if index + 1 < len(schema["fields"]) else ""
        lines.append(f"            {cpp_field_spec(field)}{comma}")
    lines.append("        };")
    lines.append("    }")
    lines.append("")
    lines.append(f"    static mt::Json to_json(const {class_name}& row)")
    lines.append("    {")
    lines.append("        return mt::Json::object(")
    lines.append("            {")
    for index, field in enumerate(schema["fields"]):
        comma = "," if index + 1 < len(schema["fields"]) else ""
        value_expr = cpp_to_json_expr(field["type"], f"row.{field['name']}")
        lines.append(f"                {{{cpp_string(field['name'])}, {value_expr}}}{comma}")
    lines.append("            }")
    lines.append("        );")
    lines.append("    }")

    if array_inner_types(schema):
        lines.append("")
        lines.append("    template <class T>")
        lines.append("    static mt::Json to_json_array(const std::vector<T>& values)")
        lines.append("    {")
        lines.append("        mt::Json::Array array;")
        lines.append("        array.reserve(values.size());")
        lines.append("        for (const auto& value : values)")
        lines.append("        {")
        lines.append("            array.push_back(mt::Json(value));")
        lines.append("        }")
        lines.append("        return mt::Json::array(std::move(array));")
        lines.append("    }")

        for type_name in array_inner_types(schema):
            type_desc = ScalarType(type_name)
            lines.append("")
            lines.append(
                f"    static std::vector<{cpp_type(type_desc)}> from_json_array_{type_name}(const mt::Json& json)"
            )
            lines.append("    {")
            lines.append(f"        std::vector<{cpp_type(type_desc)}> values;")
            lines.append("        const auto& array = json.as_array();")
            lines.append("        values.reserve(array.size());")
            lines.append("        for (const auto& item : array)")
            lines.append("        {")
            lines.append(f"            values.push_back(item.{json_accessor(type_desc)});")
            lines.append("        }")
            lines.append("        return values;")
            lines.append("    }")

    if object_types(schema):
        for index, type_desc in enumerate(object_types(schema)):
            lines.append("")
            append_object_json_helpers(lines, type_desc)

    lines.append("")
    lines.append("    static " + class_name + " from_json(const mt::Json& json)")
    lines.append("    {")
    lines.append(f"        return {class_name}{{")
    for index, field in enumerate(schema["fields"]):
        comma = "," if index + 1 < len(schema["fields"]) else ""
        json_expr = f"json[{cpp_string(field['name'])}]"
        lines.append(f"            .{field['name']} = {json_to_cpp_expr(field['type'], json_expr)}{comma}")
    lines.append("        };")
    lines.append("    }")

    if schema["indexes"]:
        lines.append("")
        lines.append("    static std::vector<mt::IndexSpec> indexes()")
        lines.append("    {")
        lines.append("        return {")
        for index, item in enumerate(schema["indexes"]):
            comma = "," if index + 1 < len(schema["indexes"]) else ""
            expr = (
                f"mt::IndexSpec::json_path_index({cpp_string(item['name'])}, "
                f"{cpp_string(item['path'])})"
            )
            if item["unique"]:
                expr += ".make_unique()"
            lines.append(f"            {expr}{comma}")
        lines.append("        };")
        lines.append("    }")

    lines.append("};")

    if schema["namespace"]:
        lines.append("")
        lines.append(f"}} // namespace {schema['namespace']}")

    lines.append("")
    return "\n".join(lines)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate mt row and mapping headers from JSON metadata.")
    parser.add_argument("schema", type=Path, help="Path to a .mt.json schema file")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Output header path")
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        with args.schema.open("r", encoding="utf-8") as f:
            raw_schema = json.load(f)
        schema = validate_schema(raw_schema)
        output = render(schema)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
    except SchemaError as exc:
        print(f"mt_codegen: schema error: {exc}", file=sys.stderr)
        return 1
    except (OSError, json.JSONDecodeError) as exc:
        print(f"mt_codegen: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
