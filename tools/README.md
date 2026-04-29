# `mt_codegen.py` Schema Format

`mt_codegen.py` generates a C++ row struct and its matching mt table mapping type from
JSON metadata.

The library repository only keeps example schemas for documentation and tests. Users
should keep application schemas in their own project and pass local paths to the tool.

## Usage

```sh
python3 tools/mt_codegen.py ./my_schemas/user.mt.json -o ./generated/user.hpp
```

The generated header includes:

```cpp
#include "mt/core.hpp"
```

Generated mapping types expose schema metadata for backend schema tracking:

- `table_name`: logical mt table name
- `schema_version`: declared schema version
- `key_field`: document key field name
- `fields()`: field definitions, including nested object fields, default values, optional
  scalar fields, and array scalar fields
- `indexes()`: declared indexes when the schema defines them

Compile generated code with the library include path:

```sh
c++ -std=c++20 -Iinclude ...
```

## Top-Level Object

Required fields:

- `table_name`: logical mt table name.
- `class_name`: generated C++ struct name.
- `key`: field name used as the document key.
- `fields`: non-empty array of field definitions.

Optional fields:

- `namespace`: C++ namespace for generated types. May use `::` separators.
- `schema_version`: positive integer. Defaults to `1`.
- `indexes`: array of index definitions. Defaults to empty.

## Field Definitions

Each field object requires:

- `name`: C++ field name.
- `type`: one of `string`, `bool`, `int64`, `double`, `optional`, `array`, or `object`.

Optional and array fields require:

- `value_type`: scalar value type for `optional` or `array`, one of `string`, `bool`, `int64`, or
  `double`.

Object fields require:

- `class_name`: generated nested C++ struct name.
- `fields`: non-empty array of nested field definitions.

Nested object fields may contain scalar, `optional`, `array`, and other `object` fields.
`optional` and `array` currently accept scalar `value_type` values only.

Optional field properties:

- `required`: boolean marker for schema documentation. Defaults to `true` unless
  `default` is present. Generated mapping metadata exposes this marker, but row
  decoding still relies on `mt::Json` accessors for missing-field failures.
- `default`: default value for the generated C++ field.

Default value types must match the field type:

- `string`: JSON string
- `bool`: JSON boolean
- `int64`: JSON integer
- `double`: JSON number

Generated C++ type mapping:

| Schema Type | C++ Type |
| --- | --- |
| `string` | `std::string` |
| `bool` | `bool` |
| `int64` | `std::int64_t` |
| `double` | `double` |
| `optional` | `std::optional<T>` |
| `array` | `std::vector<T>` |
| `object` | nested generated struct |

Optional fields are encoded as their scalar value when set and as JSON null when empty.
Array fields are encoded as JSON arrays.
Object fields are encoded as JSON objects.

## Index Definitions

Each index object requires:

- `name`: index name.
- `path`: top-level generated field path such as `$.email`.

Optional index properties:

- `unique`: boolean. Defaults to `false`.

Example:

```json
{"name": "email", "path": "$.email", "unique": true}
```

## Example Schema

```json
{
  "namespace": "app",
  "table_name": "users",
  "class_name": "User",
  "key": "id",
  "schema_version": 1,
  "fields": [
    {"name": "id", "type": "string", "required": true},
    {"name": "email", "type": "string", "required": true},
    {"name": "name", "type": "string", "required": true},
    {"name": "nickname", "type": "optional", "value_type": "string"},
    {"name": "tags", "type": "array", "value_type": "string"},
    {
      "name": "address",
      "type": "object",
      "class_name": "Address",
      "fields": [
        {"name": "city", "type": "string"},
        {"name": "postal_code", "type": "string"},
        {"name": "unit", "type": "optional", "value_type": "string"},
        {"name": "labels", "type": "array", "value_type": "string"}
      ]
    },
    {"name": "active", "type": "bool", "default": true},
    {"name": "login_count", "type": "int64", "default": 0}
  ],
  "indexes": [
    {"name": "email", "path": "$.email", "unique": true},
    {"name": "active", "path": "$.active"}
  ]
}
```

The repository test fixture is:

```text
examples/schemas/user.mt.json
```

## Validation Rules

The generator fails if:

- the schema is not a JSON object
- required top-level fields are missing
- `class_name`, field names, or namespace segments are not valid C++ identifiers
- field names are duplicated
- the `key` field does not exist
- a field type is unsupported
- an optional field is missing `value_type`
- an optional `value_type` is unsupported
- an array field is missing `value_type`
- an array `value_type` is unsupported
- an object field is missing `class_name` or `fields`
- duplicate object class names are declared with different fields
- a default value does not match the field type
- `schema_version` is not a positive integer
- index names are duplicated
- index paths do not reference declared top-level generated fields
- `unique` or `required` values are not booleans

Schema validation errors are reported without a Python traceback:

```text
mt_codegen: schema error: missing required field: class_name
```

## Current Limitations

- no enum support
- no custom includes or custom C++ type overrides
- `optional` and `array` value types are limited to scalar generated types
- index paths are limited to top-level generated fields
- `required` is metadata only; missing fields currently fail through `mt::Json` accessors

The core `mt::Json` type supports null and array values.
