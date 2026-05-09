# mt Codegen JSON Schema

`tools/mt_codegen.py` consumes user-owned `*.mt.json` metadata files and generates a
C++ row struct plus a mapping class. The portable structural contract is defined by
JSON Schema Draft 2020-12 in:

```text
schemas/mt-codegen.schema.json
```

`tools/mt_codegen.py` validates input metadata against this schema before running its
semantic validation. The same schema can also be used for editor integration and CI
checks outside the generator.

## Dialect

The schema uses JSON Schema Draft 2020-12:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema"
}
```

## Top-Level Shape

Required top-level members:

- `table_name`: logical user table name.
- `class_name`: generated C++ row struct name.
- `key`: single key field name, or a composite key object.
- `fields`: non-empty array of generated fields.

Optional top-level members:

- `namespace`: C++ namespace, using `::` between nested namespace segments. Defaults to
  no namespace.
- `schema_version`: positive integer. Defaults to `1`.
- `indexes`: array of index descriptors, or `null`. Defaults to no indexes.

## Table Names

`table_name` is the logical user table name exposed by generated mappings. Backends map
that name one-to-one to a backend-owned physical row store:

```text
users -> mt_user_users
orders -> mt_user_orders
```

The physical prefix keeps user row stores out of the backend-private `mt_*` metadata
namespace while preserving a direct mapping from each user table to exactly one physical
store.

`table_name` must:

- match `^[a-z][a-z0-9_]*$`
- be at most 39 characters
- not start with `mt_`
- not be a SQL reserved keyword such as `select`, `table`, `order`, or `where`

## Fields

Each field requires:

- `name`: C++ identifier used as the generated struct member name.
- `type`: one of `string`, `bool`, `int64`, `double`, `json`, `optional`, `array`, or
  `object`.

Scalar and JSON fields do not use `value_type`, `class_name`, or nested `fields`.

`optional` fields require `value_type`, which may be `string`, `bool`, `int64`,
`double`, or `json`.

`array` fields require `value_type`. Arrays may contain scalar values, `json`, or
objects. Arrays of objects use:

```json
{
  "name": "logins",
  "type": "array",
  "value_type": "object",
  "class_name": "Login",
  "fields": [
    {"name": "provider", "type": "string"}
  ]
}
```

`object` fields require `class_name` and a non-empty nested `fields` array.

## Keys

A single-field key is a string:

```json
"key": "id"
```

A composite key is an object with at least two unique field names and an optional
separator:

```json
"key": {"fields": ["tenant_id", "order_id", "revision"], "separator": ":"}
```

The generator requires every key field to exist and to be a `string` or `int64` field.

## Indexes

Each index requires:

- `name`: non-empty index name.
- `path`: a top-level generated field path such as `$.email`.

Optional:

- `unique`: boolean, defaulting to `false`.

The generator requires index paths to reference declared top-level fields. Unique
indexes must target required scalar fields that are not nullable.

## Semantic Checks

Some rules depend on relationships between fields and remain implemented in
`tools/mt_codegen.py` rather than JSON Schema:

- field names must be unique within each field array
- duplicate object `class_name` values must use identical nested fields
- key fields must reference declared fields
- key fields must be `string` or `int64`
- index names must be unique
- index paths must reference declared top-level fields
- unique indexes must target required scalar fields
- default values must match generated field types
