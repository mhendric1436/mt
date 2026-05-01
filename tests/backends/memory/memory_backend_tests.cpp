#include "memory_test_support.hpp"

#include <iostream>

void test_memory_backend_reports_capabilities();
void test_memory_backend_stores_schema_snapshot_on_create();
void test_memory_backend_updates_schema_snapshot_for_compatible_repeat_ensure();
void test_memory_backend_rejects_incompatible_schema_change();
void test_memory_backend_accepts_optional_field_schema_change();
void test_memory_backend_accepts_defaulted_field_schema_change();
void test_memory_backend_accepts_nested_object_schema_change();
void test_memory_backend_rejects_key_field_schema_change();
void test_memory_backend_rejects_field_type_schema_change();
void test_memory_backend_rejects_required_added_field_schema_change();
void test_memory_backend_rejects_migrations();
void test_memory_backend_active_transaction_lifecycle_allows_register_commit_and_abort();
void test_memory_backend_snapshot_reads_select_best_visible_version();
void test_memory_backend_failed_multi_write_commit_publishes_no_partial_writes();
void test_memory_backend_clock_advances_only_after_successful_commit();
void test_memory_backend_query_current_metadata_filters_and_limits();
void test_memory_backend_query_supports_json_equals();
void test_memory_backend_rejects_json_contains_query();
void test_memory_backend_rejects_non_key_ordering();
void test_memory_backend_enforces_unique_indexes();
void test_memory_backend_unique_indexes_allow_same_key_missing_path_and_delete();

int main()
{
    test_memory_backend_reports_capabilities();
    test_memory_backend_stores_schema_snapshot_on_create();
    test_memory_backend_updates_schema_snapshot_for_compatible_repeat_ensure();
    test_memory_backend_rejects_incompatible_schema_change();
    test_memory_backend_accepts_optional_field_schema_change();
    test_memory_backend_accepts_defaulted_field_schema_change();
    test_memory_backend_accepts_nested_object_schema_change();
    test_memory_backend_rejects_key_field_schema_change();
    test_memory_backend_rejects_field_type_schema_change();
    test_memory_backend_rejects_required_added_field_schema_change();
    test_memory_backend_rejects_migrations();
    test_memory_backend_active_transaction_lifecycle_allows_register_commit_and_abort();
    test_memory_backend_snapshot_reads_select_best_visible_version();
    test_memory_backend_failed_multi_write_commit_publishes_no_partial_writes();
    test_memory_backend_clock_advances_only_after_successful_commit();
    test_memory_backend_query_current_metadata_filters_and_limits();
    test_memory_backend_query_supports_json_equals();
    test_memory_backend_rejects_json_contains_query();
    test_memory_backend_rejects_non_key_ordering();
    test_memory_backend_enforces_unique_indexes();
    test_memory_backend_unique_indexes_allow_same_key_missing_path_and_delete();

    std::cout << "All memory backend tests passed.\n";
    return 0;
}
