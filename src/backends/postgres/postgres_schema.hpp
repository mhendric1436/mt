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

    static std::string select_collection_spec_by_logical_name();
    static std::string select_collection_descriptor_by_logical_name();
    static std::string insert_collection();
    static std::string insert_schema_snapshot();
    static std::string update_collection();
    static std::string update_schema_snapshot();
    static std::string select_collection_indexes_by_id();
};

void bootstrap_schema(
    Connection& connection,
    const BootstrapSpec& spec
);

std::optional<CollectionSpec> load_collection_spec(
    Connection& connection,
    std::string_view logical_name
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

} // namespace mt::backends::postgres::detail
