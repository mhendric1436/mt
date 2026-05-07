#pragma once

#include "postgres_connection.hpp"

#include "mt/collection.hpp"

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace mt::backends::postgres::detail
{

struct PrivateSchemaSql
{
    static std::string create_metadata_table();
    static std::string upsert_metadata_schema_version();
    static std::string create_clock_table();
    static std::string create_transaction_id_sequence();
    static std::string insert_default_clock_row();
    static std::string create_collections_table();
    static std::string create_schema_snapshots_table();
    static std::string create_active_transactions_table();
    static std::string create_history_table();
    static std::string create_history_snapshot_index();
    static std::string create_current_table();

    static std::string count_private_tables();
    static std::string select_metadata_schema_version();
    static std::string select_clock_row();
    static std::string begin_transaction();
    static std::string commit();
    static std::string rollback();
    static std::string select_clock_version();
    static std::string lock_clock_version();
    static std::string increment_clock_version_returning();
    static std::string next_transaction_id();
    static std::string insert_or_replace_active_transaction();
    static std::string delete_active_transaction();
    static std::string count_active_transactions();
    static std::string select_snapshot_document();
    static std::string select_current_metadata();
    static std::string select_snapshot_list(
        bool has_after_key,
        bool has_limit
    );
    static std::string select_current_metadata_list(
        bool has_after_key,
        bool has_limit
    );
    static std::string select_current_query_candidates(bool has_after_key);
    static std::string select_current_query_by_index(
        std::string_view field_name,
        bool has_after_key
    );
    static std::string select_current_unique_index_candidates();
    static std::string insert_history();
    static std::string upsert_current();
    static std::string select_history_row_by_collection_key();
    static std::string select_current_row_by_collection_key();
    static std::string count_history_rows();

    static std::string select_collection_spec_by_logical_name();
    static std::string select_collection_spec_by_id();
    static std::string select_collection_descriptor_by_logical_name();
    static std::string insert_collection();
    static std::string insert_schema_snapshot();
    static std::string update_collection();
    static std::string update_schema_snapshot();
    static std::string select_collection_indexes_by_id();
    static std::string select_current_document_with_missing_index_value();
    static std::string count_physical_index_by_name();
};

void bootstrap_schema(
    Connection& connection,
    const BootstrapSpec& spec
);

std::optional<CollectionSpec> load_collection_spec(
    Connection& connection,
    std::string_view logical_name
);

CollectionSpec load_collection_spec(
    Connection& connection,
    CollectionId collection
);

CollectionDescriptor load_collection_descriptor(
    Connection& connection,
    std::string_view logical_name
);

CollectionDescriptor insert_collection(
    Connection& connection,
    const CollectionSpec& spec
);

void update_collection(
    Connection& connection,
    const CollectionSpec& spec
);

std::vector<IndexSpec> load_collection_indexes(
    Connection& connection,
    CollectionId collection
);

std::string physical_unique_index_name(
    CollectionId collection,
    const IndexSpec& index
);

void validate_postgres_index_update(
    const CollectionSpec& existing,
    const CollectionSpec& requested
);

void create_physical_unique_indexes(
    Connection& connection,
    CollectionId collection,
    const CollectionSpec& spec
);

void create_added_physical_unique_indexes(
    Connection& connection,
    CollectionId collection,
    const CollectionSpec& existing,
    const CollectionSpec& requested
);

} // namespace mt::backends::postgres::detail
