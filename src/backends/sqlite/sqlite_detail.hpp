#pragma once

#include "mt/errors.hpp"

#include <sqlite3.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

// -----------------------------------------------------------------------------
// Private SQLite backend helpers.
//
// This header is intentionally kept under src/backends/sqlite so SQLite client
// headers do not leak into the public mt include surface.
// -----------------------------------------------------------------------------

namespace mt::backends::sqlite::detail
{

struct StoragePath
{
    static constexpr std::string_view memory()
    {
        return ":memory:";
    }
};

[[noreturn]] inline void throw_sqlite_error(
    sqlite3* db,
    std::string_view context
)
{
    std::string message(context);
    if (db != nullptr)
    {
        message += ": ";
        message += sqlite3_errmsg(db);
    }
    throw BackendError(message);
}

inline void require_sqlite_ok(
    sqlite3* db,
    int rc,
    std::string_view context
)
{
    if (rc != SQLITE_OK)
    {
        throw_sqlite_error(db, context);
    }
}

class Connection
{
  public:
    Connection() = default;

    explicit Connection(sqlite3* db) noexcept
        : db_(db)
    {
    }

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    Connection(Connection&& other) noexcept
        : db_(std::exchange(
              other.db_,
              nullptr
          ))
    {
    }

    Connection& operator=(Connection&& other) noexcept
    {
        if (this != &other)
        {
            close();
            db_ = std::exchange(other.db_, nullptr);
        }
        return *this;
    }

    ~Connection()
    {
        close();
    }

    static Connection open(std::string_view path)
    {
        sqlite3* db = nullptr;
        auto rc = sqlite3_open_v2(
            std::string(path).c_str(), &db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr
        );
        if (rc != SQLITE_OK)
        {
            auto message = std::string("open sqlite database");
            if (db != nullptr)
            {
                message += ": ";
                message += sqlite3_errmsg(db);
                sqlite3_close(db);
            }
            throw BackendError(message);
        }

        return Connection(db);
    }

    static Connection open_memory()
    {
        return open(StoragePath::memory());
    }

    sqlite3* get() const noexcept
    {
        return db_;
    }

    void execute(std::string_view sql)
    {
        char* error = nullptr;
        auto rc = sqlite3_exec(db_, std::string(sql).c_str(), nullptr, nullptr, &error);
        if (rc != SQLITE_OK)
        {
            std::string message = "execute sqlite statement";
            if (error != nullptr)
            {
                message += ": ";
                message += error;
                sqlite3_free(error);
            }
            else
            {
                message += ": ";
                message += sqlite3_errmsg(db_);
            }
            throw BackendError(message);
        }
    }

  private:
    void close() noexcept
    {
        if (db_ != nullptr)
        {
            sqlite3_close(db_);
            db_ = nullptr;
        }
    }

  private:
    sqlite3* db_ = nullptr;
};

class Statement
{
  public:
    Statement() = default;

    Statement(
        sqlite3* db,
        std::string_view sql
    )
        : db_(db)
    {
        auto rc = sqlite3_prepare_v2(db_, std::string(sql).c_str(), -1, &stmt_, nullptr);
        require_sqlite_ok(db_, rc, "prepare sqlite statement");
    }

    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    Statement(Statement&& other) noexcept
        : db_(std::exchange(
              other.db_,
              nullptr
          )),
          stmt_(
              std::exchange(
                  other.stmt_,
                  nullptr
              )
          )
    {
    }

    Statement& operator=(Statement&& other) noexcept
    {
        if (this != &other)
        {
            finalize();
            db_ = std::exchange(other.db_, nullptr);
            stmt_ = std::exchange(other.stmt_, nullptr);
        }
        return *this;
    }

    ~Statement()
    {
        finalize();
    }

    void bind_int64(
        int index,
        std::int64_t value
    )
    {
        require_sqlite_ok(db_, sqlite3_bind_int64(stmt_, index, value), "bind sqlite int64");
    }

    void bind_text(
        int index,
        std::string_view value
    )
    {
        auto destructor = reinterpret_cast<sqlite3_destructor_type>(SQLITE_TRANSIENT);
        auto rc = sqlite3_bind_text(
            stmt_, index, value.data(), static_cast<int>(value.size()), destructor
        );
        require_sqlite_ok(db_, rc, "bind sqlite text");
    }

    void bind_null(int index)
    {
        require_sqlite_ok(db_, sqlite3_bind_null(stmt_, index), "bind sqlite null");
    }

    bool step()
    {
        auto rc = sqlite3_step(stmt_);
        if (rc == SQLITE_ROW)
        {
            return true;
        }
        if (rc == SQLITE_DONE)
        {
            return false;
        }
        throw_sqlite_error(db_, "step sqlite statement");
    }

    void reset()
    {
        require_sqlite_ok(db_, sqlite3_reset(stmt_), "reset sqlite statement");
        require_sqlite_ok(db_, sqlite3_clear_bindings(stmt_), "clear sqlite bindings");
    }

    std::int64_t column_int64(int index) const
    {
        return sqlite3_column_int64(stmt_, index);
    }

    std::string column_text(int index) const
    {
        const auto* text = sqlite3_column_text(stmt_, index);
        if (text == nullptr)
        {
            return {};
        }
        return reinterpret_cast<const char*>(text);
    }

    bool column_is_null(int index) const
    {
        return sqlite3_column_type(stmt_, index) == SQLITE_NULL;
    }

  private:
    void finalize() noexcept
    {
        if (stmt_ != nullptr)
        {
            sqlite3_finalize(stmt_);
            stmt_ = nullptr;
        }
    }

  private:
    sqlite3* db_ = nullptr;
    sqlite3_stmt* stmt_ = nullptr;
};

struct PrivateSchemaSql
{
    static std::string enable_foreign_keys()
    {
        return "PRAGMA foreign_keys = ON";
    }

    static std::string begin_immediate()
    {
        return "BEGIN IMMEDIATE";
    }

    static std::string commit()
    {
        return "COMMIT";
    }

    static std::string rollback()
    {
        return "ROLLBACK";
    }

    static std::string create_meta_table()
    {
        return "CREATE TABLE IF NOT EXISTS mt_meta ("
               "key TEXT PRIMARY KEY,"
               "value INTEGER NOT NULL"
               ")";
    }

    static std::string upsert_metadata_schema_version()
    {
        return "INSERT INTO mt_meta (key, value) VALUES ('metadata_schema_version', ?) "
               "ON CONFLICT(key) DO UPDATE SET value = MAX(value, excluded.value)";
    }

    static std::string create_clock_table()
    {
        return "CREATE TABLE IF NOT EXISTS mt_clock ("
               "id INTEGER PRIMARY KEY CHECK (id = 1),"
               "version INTEGER NOT NULL,"
               "next_tx_id INTEGER NOT NULL"
               ")";
    }

    static std::string insert_default_clock_row()
    {
        return "INSERT OR IGNORE INTO mt_clock (id, version, next_tx_id) VALUES (1, 0, 1)";
    }

    static std::string create_collections_table()
    {
        return "CREATE TABLE IF NOT EXISTS mt_collections ("
               "id INTEGER PRIMARY KEY AUTOINCREMENT,"
               "logical_name TEXT NOT NULL UNIQUE,"
               "schema_version INTEGER NOT NULL,"
               "key_field TEXT NOT NULL,"
               "schema_json TEXT NOT NULL,"
               "indexes_json TEXT NOT NULL"
               ")";
    }

    static std::string create_active_transactions_table()
    {
        return "CREATE TABLE IF NOT EXISTS mt_active_transactions ("
               "tx_id TEXT PRIMARY KEY,"
               "start_version INTEGER NOT NULL"
               ")";
    }

    static std::string create_history_table()
    {
        return "CREATE TABLE IF NOT EXISTS mt_history ("
               "collection_id INTEGER NOT NULL,"
               "document_key TEXT NOT NULL,"
               "version INTEGER NOT NULL,"
               "deleted INTEGER NOT NULL,"
               "value_hash TEXT NOT NULL,"
               "value_json TEXT,"
               "PRIMARY KEY (collection_id, document_key, version),"
               "FOREIGN KEY (collection_id) REFERENCES mt_collections(id)"
               ")";
    }

    static std::string create_history_snapshot_index()
    {
        return "CREATE INDEX IF NOT EXISTS mt_history_snapshot_idx "
               "ON mt_history (collection_id, document_key, version)";
    }

    static std::string create_current_table()
    {
        return "CREATE TABLE IF NOT EXISTS mt_current ("
               "collection_id INTEGER NOT NULL,"
               "document_key TEXT NOT NULL,"
               "version INTEGER NOT NULL,"
               "deleted INTEGER NOT NULL,"
               "value_hash TEXT NOT NULL,"
               "value_json TEXT,"
               "PRIMARY KEY (collection_id, document_key),"
               "FOREIGN KEY (collection_id) REFERENCES mt_collections(id)"
               ")";
    }

    static std::string select_collection_spec_by_logical_name()
    {
        return "SELECT logical_name, schema_version, key_field, schema_json, indexes_json "
               "FROM mt_collections WHERE logical_name = ?";
    }

    static std::string select_collection_descriptor_by_logical_name()
    {
        return "SELECT id, logical_name, schema_version "
               "FROM mt_collections WHERE logical_name = ?";
    }

    static std::string insert_collection()
    {
        return "INSERT INTO mt_collections "
               "(logical_name, schema_version, key_field, schema_json, indexes_json) "
               "VALUES (?, ?, ?, ?, ?)";
    }

    static std::string update_collection()
    {
        return "UPDATE mt_collections "
               "SET schema_version = ?, key_field = ?, schema_json = ?, indexes_json = ? "
               "WHERE logical_name = ?";
    }

    static std::string select_collection_indexes_by_id()
    {
        return "SELECT indexes_json FROM mt_collections WHERE id = ?";
    }

    static std::string select_current_unique_index_candidates()
    {
        return "SELECT document_key, value_json "
               "FROM mt_current "
               "WHERE collection_id = ? AND deleted = 0 AND document_key <> ?";
    }

    static std::string select_clock_version()
    {
        return "SELECT version FROM mt_clock WHERE id = 1";
    }

    static std::string select_next_tx_id()
    {
        return "SELECT next_tx_id FROM mt_clock WHERE id = 1";
    }

    static std::string increment_clock_version()
    {
        return "UPDATE mt_clock SET version = version + 1 WHERE id = 1";
    }

    static std::string increment_next_tx_id()
    {
        return "UPDATE mt_clock SET next_tx_id = next_tx_id + 1 WHERE id = 1";
    }

    static std::string insert_or_replace_active_transaction()
    {
        return "INSERT OR REPLACE INTO mt_active_transactions (tx_id, start_version) "
               "VALUES (?, ?)";
    }

    static std::string delete_active_transaction()
    {
        return "DELETE FROM mt_active_transactions WHERE tx_id = ?";
    }

    static std::string select_snapshot_document()
    {
        return "SELECT version, deleted, value_hash, value_json "
               "FROM mt_history "
               "WHERE collection_id = ? AND document_key = ? AND version <= ? "
               "ORDER BY version DESC LIMIT 1";
    }

    static std::string select_current_metadata()
    {
        return "SELECT version, deleted, value_hash "
               "FROM mt_current "
               "WHERE collection_id = ? AND document_key = ?";
    }

    static std::string select_current_query_candidates(bool has_after_key)
    {
        auto sql = std::string(
            "SELECT document_key, version, deleted, value_hash, value_json "
            "FROM mt_current "
            "WHERE collection_id = ? "
        );
        if (has_after_key)
        {
            sql += "AND document_key > ? ";
        }
        sql += "ORDER BY document_key";
        return sql;
    }

    static std::string select_snapshot_list(
        bool has_after_key,
        bool has_limit
    )
    {
        auto sql = std::string(
            "SELECT h.document_key, h.version, h.deleted, h.value_hash, h.value_json "
            "FROM mt_history h "
            "WHERE h.collection_id = ? "
            "AND h.version = ("
            "SELECT MAX(h2.version) FROM mt_history h2 "
            "WHERE h2.collection_id = h.collection_id "
            "AND h2.document_key = h.document_key "
            "AND h2.version <= ?"
            ") "
            "AND h.deleted = 0 "
        );
        if (has_after_key)
        {
            sql += "AND h.document_key > ? ";
        }
        sql += "ORDER BY h.document_key ";
        if (has_limit)
        {
            sql += "LIMIT ?";
        }
        return sql;
    }

    static std::string select_current_metadata_list(
        bool has_after_key,
        bool has_limit
    )
    {
        auto sql = std::string(
            "SELECT document_key, version, deleted, value_hash "
            "FROM mt_current "
            "WHERE collection_id = ? "
        );
        if (has_after_key)
        {
            sql += "AND document_key > ? ";
        }
        sql += "ORDER BY document_key ";
        if (has_limit)
        {
            sql += "LIMIT ?";
        }
        return sql;
    }

    static std::string insert_history()
    {
        return "INSERT INTO mt_history "
               "(collection_id, document_key, version, deleted, value_hash, value_json) "
               "VALUES (?, ?, ?, ?, ?, ?)";
    }

    static std::string upsert_current()
    {
        return "INSERT INTO mt_current "
               "(collection_id, document_key, version, deleted, value_hash, value_json) "
               "VALUES (?, ?, ?, ?, ?, ?) "
               "ON CONFLICT(collection_id, document_key) DO UPDATE SET "
               "version = excluded.version, "
               "deleted = excluded.deleted, "
               "value_hash = excluded.value_hash, "
               "value_json = excluded.value_json";
    }

    static std::string count_private_tables()
    {
        return "SELECT COUNT(*) FROM sqlite_master "
               "WHERE type = 'table' "
               "AND name IN ("
               "'mt_meta', 'mt_clock', 'mt_collections', "
               "'mt_active_transactions', 'mt_history', 'mt_current')";
    }

    static std::string select_metadata_schema_version()
    {
        return "SELECT value FROM mt_meta WHERE key = 'metadata_schema_version'";
    }

    static std::string select_clock_row()
    {
        return "SELECT version, next_tx_id FROM mt_clock WHERE id = 1";
    }

    static std::string count_active_transactions()
    {
        return "SELECT COUNT(*) FROM mt_active_transactions";
    }

    static std::string select_history_row_by_collection_key()
    {
        return "SELECT version, deleted, value_hash, value_json FROM mt_history "
               "WHERE collection_id = ? AND document_key = ?";
    }

    static std::string select_current_row_by_collection_key()
    {
        return "SELECT version, deleted, value_hash, value_json FROM mt_current "
               "WHERE collection_id = ? AND document_key = ?";
    }

    static std::string count_history_rows()
    {
        return "SELECT COUNT(*) FROM mt_history";
    }
};

} // namespace mt::backends::sqlite::detail
