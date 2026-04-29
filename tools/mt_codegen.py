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


def parse_type(type_name, context):
    if type_name not in SUPPORTED_TYPES:
        fail(f"unsupported type for {context}: {type_name!r}")
    return ScalarType(type_name)


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

    fields = schema.get("fields")
    if not isinstance(fields, list) or not fields:
        fail("fields must be a non-empty array")

    seen = set()
    validated_fields = []
    for index, field in enumerate(fields):
        field = require_object(field, f"fields[{index}]")
        name = require_named_string(field, "name", "field.name")
        validate_identifier(name, "field name")
        if name in seen:
            fail(f"duplicate field name {name!r}")
        seen.add(name)

        field_type = require_named_string(field, "type", "field.type")
        type_desc = parse_type(field_type, f"field {name!r}")

        if "required" in field and not isinstance(field["required"], bool):
            fail(f"required for field {name!r} must be a bool")
        if "default" in field:
            cpp_default_value(type_desc, field["default"], name)

        validated_fields.append(
            {
                "name": name,
                "type": type_desc,
                "required": field.get("required", "default" not in field),
                "default": field.get("default"),
                "has_default": "default" in field,
            }
        )

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

    if schema["namespace"]:
        lines.append(f"namespace {schema['namespace']}")
        lines.append("{")
        lines.append("")

    lines.append(f"struct {class_name}")
    lines.append("{")
    for field in schema["fields"]:
        lines.append(f"    {cpp_type(field['type'])} {field['name']}{cpp_default(field)};")
    lines.append("")
    lines.append(f"    friend bool operator==(const {class_name}&, const {class_name}&) = default;")
    lines.append("};")
    lines.append("")

    lines.append(f"struct {mapping_name}")
    lines.append("{")
    lines.append(f"    static constexpr std::string_view table_name = {cpp_string(schema['table_name'])};")
    lines.append(f"    static constexpr int schema_version = {schema['schema_version']};")
    lines.append("")
    lines.append(f"    static std::string key(const {class_name}& row)")
    lines.append("    {")
    lines.append(f"        return row.{schema['key']};")
    lines.append("    }")
    lines.append("")
    lines.append(f"    static mt::Json to_json(const {class_name}& row)")
    lines.append("    {")
    lines.append("        return mt::Json::object(")
    lines.append("            {")
    for index, field in enumerate(schema["fields"]):
        comma = "," if index + 1 < len(schema["fields"]) else ""
        lines.append(f"                {{{cpp_string(field['name'])}, row.{field['name']}}}{comma}")
    lines.append("            }")
    lines.append("        );")
    lines.append("    }")
    lines.append("")
    lines.append("    static " + class_name + " from_json(const mt::Json& json)")
    lines.append("    {")
    lines.append(f"        return {class_name}{{")
    for index, field in enumerate(schema["fields"]):
        comma = "," if index + 1 < len(schema["fields"]) else ""
        lines.append(
            f"            .{field['name']} = json[{cpp_string(field['name'])}].{json_accessor(field['type'])}{comma}"
        )
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
