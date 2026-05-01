#include "sqlite_test_support.hpp"

#include <iostream>

void test_sqlite_backend_reports_capabilities();
void test_sqlite_backend_rejects_unimplemented_operations();
void test_sqlite_backend_bootstrap_creates_private_metadata();
void test_sqlite_backend_bootstrap_runs_once_per_backend_instance();
void test_sqlite_backend_ensure_collection_creates_and_gets_descriptor();
void test_sqlite_backend_ensure_collection_persists_across_instances();
void test_sqlite_backend_accepts_compatible_schema_change();
void test_sqlite_backend_rejects_incompatible_schema_change();
void test_sqlite_backend_session_commits_and_aborts_transactions();
void test_sqlite_backend_session_rejects_invalid_lifecycle();
void test_sqlite_backend_session_rejects_unsupported_queries();
void test_sqlite_backend_clock_reads_and_increments();
void test_sqlite_backend_clock_increment_requires_lock();
void test_sqlite_backend_transaction_ids_are_unique_and_persisted();
void test_sqlite_backend_active_transaction_metadata_registers_and_cleans_up();
void test_sqlite_backend_document_writes_insert_history_and_current();
void test_sqlite_backend_document_writes_store_delete_tombstone();
void test_sqlite_backend_document_writes_rollback_on_abort();
void test_sqlite_backend_point_reads_return_snapshot_versions();
void test_sqlite_backend_point_reads_return_tombstones();
void test_sqlite_backend_list_snapshot_orders_and_paginates();
void test_sqlite_backend_list_current_metadata_orders_and_paginates();
void test_sqlite_backend_query_snapshot_supports_key_prefix_and_json_equals();
void test_sqlite_backend_query_current_metadata_filters_and_limits();
void test_sqlite_backend_enforces_unique_indexes();
void test_sqlite_backend_unique_indexes_allow_same_key_update_missing_path_and_delete();
void test_sqlite_core_transactions_persist_across_backend_instances();
void test_sqlite_core_table_list_query_and_delete();
void test_sqlite_core_table_list_limit_skips_tombstones();
void test_sqlite_core_transactions_reject_unique_index_conflict();
void test_sqlite_core_rejects_unsupported_query();
void test_sqlite_detail_connection_executes_sql();
void test_sqlite_detail_statement_reuses_bindings_after_reset();
void test_sqlite_detail_statement_binds_text_and_null();

int main()
{
    test_sqlite_backend_reports_capabilities();
    test_sqlite_backend_rejects_unimplemented_operations();
    test_sqlite_backend_bootstrap_creates_private_metadata();
    test_sqlite_backend_bootstrap_runs_once_per_backend_instance();
    test_sqlite_backend_ensure_collection_creates_and_gets_descriptor();
    test_sqlite_backend_ensure_collection_persists_across_instances();
    test_sqlite_backend_accepts_compatible_schema_change();
    test_sqlite_backend_rejects_incompatible_schema_change();
    test_sqlite_backend_session_commits_and_aborts_transactions();
    test_sqlite_backend_session_rejects_invalid_lifecycle();
    test_sqlite_backend_session_rejects_unsupported_queries();
    test_sqlite_backend_clock_reads_and_increments();
    test_sqlite_backend_clock_increment_requires_lock();
    test_sqlite_backend_transaction_ids_are_unique_and_persisted();
    test_sqlite_backend_active_transaction_metadata_registers_and_cleans_up();
    test_sqlite_backend_document_writes_insert_history_and_current();
    test_sqlite_backend_document_writes_store_delete_tombstone();
    test_sqlite_backend_document_writes_rollback_on_abort();
    test_sqlite_backend_point_reads_return_snapshot_versions();
    test_sqlite_backend_point_reads_return_tombstones();
    test_sqlite_backend_list_snapshot_orders_and_paginates();
    test_sqlite_backend_list_current_metadata_orders_and_paginates();
    test_sqlite_backend_query_snapshot_supports_key_prefix_and_json_equals();
    test_sqlite_backend_query_current_metadata_filters_and_limits();
    test_sqlite_backend_enforces_unique_indexes();
    test_sqlite_backend_unique_indexes_allow_same_key_update_missing_path_and_delete();
    test_sqlite_core_transactions_persist_across_backend_instances();
    test_sqlite_core_table_list_query_and_delete();
    test_sqlite_core_table_list_limit_skips_tombstones();
    test_sqlite_core_transactions_reject_unique_index_conflict();
    test_sqlite_core_rejects_unsupported_query();
    test_sqlite_detail_connection_executes_sql();
    test_sqlite_detail_statement_reuses_bindings_after_reset();
    test_sqlite_detail_statement_binds_text_and_null();

    std::cout << "All sqlite backend tests passed.\n";
    return 0;
}
