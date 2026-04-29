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
        self.assertEqual(validated["key"], "id")
        self.assertEqual(len(validated["fields"]), 3)

    def test_valid_schema_parses_field_type_descriptors(self):
        validated = mt_codegen.validate_schema(copy.deepcopy(VALID_SCHEMA))

        self.assertEqual(validated["fields"][0]["type"], mt_codegen.ScalarType("string"))
        self.assertEqual(validated["fields"][2]["type"], mt_codegen.ScalarType("bool"))

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
        schema["fields"].append({"name": "items", "type": "array", "value_type": "object"})

        self.assert_schema_error(schema, "unsupported type for fields[3].value_type: 'object'")

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
