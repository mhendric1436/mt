#!/usr/bin/env python3

import copy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import mt_codegen


VALID_SCHEMA = {
    "namespace": "mt_examples",
    "table_name": "users",
    "class_name": "User",
    "key": "id",
    "schema_version": 1,
    "fields": [
        {"name": "id", "type": "string", "required": True},
        {"name": "email", "type": "string", "required": True},
        {"name": "active", "type": "bool", "default": True},
    ],
    "indexes": [
        {"name": "email", "path": "$.email", "unique": True},
    ],
}


class SchemaValidationTests(unittest.TestCase):
    def assert_schema_error(self, schema, expected):
        with self.assertRaises(mt_codegen.SchemaError) as raised:
            mt_codegen.validate_schema(schema)
        self.assertIn(expected, str(raised.exception))

    def test_valid_schema_passes(self):
        validated = mt_codegen.validate_schema(copy.deepcopy(VALID_SCHEMA))

        self.assertEqual(validated["class_name"], "User")
        self.assertEqual(validated["key"], mt_codegen.SingleKey("id"))
        self.assertEqual(validated["key_field"], "id")
        self.assertEqual(len(validated["fields"]), 3)

    def test_valid_schema_parses_field_type_descriptors(self):
        validated = mt_codegen.validate_schema(copy.deepcopy(VALID_SCHEMA))

        self.assertEqual(validated["fields"][0]["type"], mt_codegen.ScalarType("string"))
        self.assertEqual(validated["fields"][2]["type"], mt_codegen.ScalarType("bool"))

    def test_render_emits_schema_metadata(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "tags", "type": "array", "value_type": "string"})
        schema["fields"].append(
            {
                "name": "address",
                "type": "object",
                "class_name": "Address",
                "fields": [{"name": "city", "type": "string"}],
            }
        )
        validated = mt_codegen.validate_schema(schema)

        rendered = mt_codegen.render(validated)

        self.assertIn('static constexpr std::string_view key_field = "id";', rendered)
        self.assertIn("static std::vector<mt::FieldSpec> fields()", rendered)
        self.assertIn('mt::FieldSpec::string("id").mark_required(true)', rendered)
        self.assertIn('mt::FieldSpec::array("tags", mt::FieldType::String)', rendered)
        self.assertIn('mt::FieldSpec::object("address"', rendered)
        self.assertIn('mt::FieldSpec::boolean("active")', rendered)
        self.assertIn(".with_default(mt::Json(true))", rendered)

    def test_json_field_parses_type_descriptor(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "metadata", "type": "json", "default": {"role": "admin"}})

        validated = mt_codegen.validate_schema(schema)
        rendered = mt_codegen.render(validated)

        self.assertEqual(validated["fields"][3]["type"], mt_codegen.JsonType())
        self.assertIn("mt::Json metadata = mt::Json::object", rendered)
        self.assertIn('mt::FieldSpec::json("metadata")', rendered)
        self.assertIn('.with_default(mt::Json::object({mt::Json::Member{"role", mt::Json("admin")}}))', rendered)

    def test_optional_scalar_field_parses_type_descriptor(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "nickname", "type": "optional", "value_type": "string"})

        validated = mt_codegen.validate_schema(schema)

        self.assertEqual(
            validated["fields"][3]["type"],
            mt_codegen.OptionalType(mt_codegen.ScalarType("string")),
        )

    def test_optional_field_requires_value_type(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "nickname", "type": "optional"})

        self.assert_schema_error(schema, "fields[3].value_type must be a non-empty string")

    def test_optional_field_rejects_unsupported_value_type(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "metadata", "type": "optional", "value_type": "object"})

        self.assert_schema_error(schema, "unsupported type for fields[3].value_type: 'object'")

    def test_array_scalar_field_parses_type_descriptor(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "tags", "type": "array", "value_type": "string"})

        validated = mt_codegen.validate_schema(schema)

        self.assertEqual(
            validated["fields"][3]["type"],
            mt_codegen.ArrayType(mt_codegen.ScalarType("string")),
        )

    def test_array_field_requires_value_type(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "tags", "type": "array"})

        self.assert_schema_error(schema, "fields[3].value_type must be a non-empty string")

    def test_array_field_rejects_unsupported_value_type(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "items", "type": "array", "value_type": "map"})

        self.assert_schema_error(schema, "unsupported type for fields[3].value_type: 'map'")

    def test_array_object_field_parses_type_descriptor(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append(
            {
                "name": "items",
                "type": "array",
                "value_type": "object",
                "class_name": "LineItem",
                "fields": [
                    {"name": "sku", "type": "string"},
                    {"name": "quantity", "type": "int64"},
                ],
            }
        )

        validated = mt_codegen.validate_schema(schema)
        rendered = mt_codegen.render(validated)

        self.assertEqual(validated["fields"][3]["type"].inner.class_name, "LineItem")
        self.assertIn("std::vector<LineItem> items", rendered)
        self.assertIn('mt::FieldSpec::array_object("items"', rendered)
        self.assertIn("static mt::Json to_json_array_LineItem", rendered)
        self.assertIn("static std::vector<LineItem> from_json_array_LineItem", rendered)

    def test_object_field_parses_type_descriptor(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append(
            {
                "name": "address",
                "type": "object",
                "class_name": "Address",
                "fields": [
                    {"name": "city", "type": "string"},
                    {"name": "postal_code", "type": "string"},
                ],
            }
        )

        validated = mt_codegen.validate_schema(schema)

        self.assertEqual(validated["fields"][3]["type"].class_name, "Address")
        self.assertEqual(len(validated["fields"][3]["type"].fields), 2)

    def test_object_field_supports_nested_optional_and_array_fields(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append(
            {
                "name": "address",
                "type": "object",
                "class_name": "Address",
                "fields": [
                    {"name": "city", "type": "string"},
                    {"name": "unit", "type": "optional", "value_type": "string"},
                    {"name": "labels", "type": "array", "value_type": "string"},
                ],
            }
        )

        validated = mt_codegen.validate_schema(schema)
        address = validated["fields"][3]["type"]

        self.assertEqual(
            address.fields[1]["type"],
            mt_codegen.OptionalType(mt_codegen.ScalarType("string")),
        )
        self.assertEqual(
            address.fields[2]["type"],
            mt_codegen.ArrayType(mt_codegen.ScalarType("string")),
        )

    def test_object_field_allows_duplicate_class_name_with_same_fields(self):
        address_fields = [{"name": "city", "type": "string"}]
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].extend(
            [
                {
                    "name": "billing",
                    "type": "object",
                    "class_name": "Address",
                    "fields": copy.deepcopy(address_fields),
                },
                {
                    "name": "shipping",
                    "type": "object",
                    "class_name": "Address",
                    "fields": copy.deepcopy(address_fields),
                },
            ]
        )

        validated = mt_codegen.validate_schema(schema)

        self.assertEqual(validated["fields"][3]["type"], validated["fields"][4]["type"])

    def test_object_field_requires_class_name(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append(
            {
                "name": "address",
                "type": "object",
                "fields": [{"name": "city", "type": "string"}],
            }
        )

        self.assert_schema_error(schema, "fields[3].class_name must be a non-empty string")

    def test_object_field_requires_fields(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "address", "type": "object", "class_name": "Address"})

        self.assert_schema_error(schema, "fields[3].fields must be a non-empty array")

    def test_object_field_rejects_duplicate_class_name_with_different_fields(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].extend(
            [
                {
                    "name": "billing",
                    "type": "object",
                    "class_name": "Address",
                    "fields": [{"name": "city", "type": "string"}],
                },
                {
                    "name": "shipping",
                    "type": "object",
                    "class_name": "Address",
                    "fields": [{"name": "postal_code", "type": "string"}],
                },
            ]
        )

        self.assert_schema_error(schema, "duplicate object class_name with different fields: 'Address'")

    def test_missing_required_top_level_field(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        del schema["class_name"]

        self.assert_schema_error(schema, "missing required field: class_name")

    def test_duplicate_field_name(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].append({"name": "email", "type": "string"})

        self.assert_schema_error(schema, "duplicate field name 'email'")

    def test_invalid_field_identifier(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"][1]["name"] = "email-address"

        self.assert_schema_error(schema, "field name must be a valid C++ identifier")

    def test_unsupported_field_type(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"][1]["type"] = "map"

        self.assert_schema_error(schema, "unsupported type for fields[1]: 'map'")

    def test_key_must_reference_declared_field(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["key"] = "missing"

        self.assert_schema_error(schema, "key field 'missing' does not exist")

    def test_composite_key_parses_and_renders(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"].insert(0, {"name": "tenant_id", "type": "string", "required": True})
        schema["fields"].append({"name": "revision", "type": "int64", "required": True})
        schema["key"] = {"fields": ["tenant_id", "id", "revision"], "separator": "::"}

        validated = mt_codegen.validate_schema(schema)
        rendered = mt_codegen.render(validated)

        self.assertEqual(
            validated["key"], mt_codegen.CompositeKey(("tenant_id", "id", "revision"), "::")
        )
        self.assertEqual(validated["key_field"], "tenant_id::id::revision")
        self.assertIn('static constexpr std::string_view key_field = "tenant_id::id::revision";', rendered)
        self.assertIn('return row.tenant_id + "::" + row.id + "::" + std::to_string(row.revision);', rendered)

    def test_composite_key_requires_declared_string_or_int64_fields(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["key"] = {"fields": ["id", "active"]}

        self.assert_schema_error(schema, "key field 'active' must be a string or int64 field")

    def test_composite_key_requires_at_least_two_fields(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["key"] = {"fields": ["id"]}

        self.assert_schema_error(schema, "key.fields must be an array with at least two field names")

    def test_schema_version_must_be_positive_integer(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["schema_version"] = 0

        self.assert_schema_error(schema, "schema_version must be a positive integer")

    def test_fields_must_be_non_empty_array(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["fields"] = []

        self.assert_schema_error(schema, "fields must be a non-empty array")

    def test_index_path_must_reference_declared_field(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["indexes"][0]["path"] = "$.missing"

        self.assert_schema_error(schema, "indexes[0].path references unknown field 'missing'")

    def test_index_path_must_be_top_level_field(self):
        schema = copy.deepcopy(VALID_SCHEMA)
        schema["indexes"][0]["path"] = "$.email.domain"

        self.assert_schema_error(schema, "indexes[0].path must reference a top-level generated field")

    def test_cli_prints_schema_error_without_traceback(self):
        with tempfile.TemporaryDirectory() as temp:
            schema_path = Path(temp) / "bad.mt.json"
            output_path = Path(temp) / "bad.hpp"
            schema_path.write_text('{"table_name": "users"}', encoding="utf-8")

            result = subprocess.run(
                [sys.executable, "tools/mt_codegen.py", str(schema_path), "-o", str(output_path)],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mt_codegen: schema error: missing required field: class_name", result.stderr)
        self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
