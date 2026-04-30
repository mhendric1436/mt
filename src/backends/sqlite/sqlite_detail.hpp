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
        return open(":memory:");
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

} // namespace mt::backends::sqlite::detail
