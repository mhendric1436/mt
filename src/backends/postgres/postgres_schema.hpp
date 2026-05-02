#pragma once

#include "postgres_connection.hpp"

#include "mt/collection.hpp"

#include <string>

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
};

void bootstrap_schema(
    Connection& connection,
    const BootstrapSpec& spec
);

} // namespace mt::backends::postgres::detail
