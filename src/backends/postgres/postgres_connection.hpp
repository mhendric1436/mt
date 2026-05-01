#pragma once

#include "mt/errors.hpp"

#include <libpq-fe.h>

#include <cstddef>
#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

// -----------------------------------------------------------------------------
// Private PostgreSQL backend connection helpers.
//
// This header is intentionally kept under src/backends/postgres so libpq client
// headers do not leak into the public mt include surface.
// -----------------------------------------------------------------------------

namespace mt::backends::postgres::detail
{

class Result
{
  public:
    Result() = default;
    explicit Result(PGresult* result) noexcept;

    Result(const Result&) = delete;
    Result& operator=(const Result&) = delete;

    Result(Result&& other) noexcept;
    Result& operator=(Result&& other) noexcept;

    ~Result();

    PGresult* get() const noexcept;
    ExecStatusType status() const noexcept;
    int rows() const noexcept;
    int columns() const noexcept;
    bool is_null(
        int row,
        int column
    ) const;
    std::string_view value(
        int row,
        int column
    ) const;

  private:
    void clear() noexcept;

  private:
    PGresult* result_ = nullptr;
};

class Connection
{
  public:
    Connection() = default;
    explicit Connection(PGconn* connection) noexcept;

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    Connection(Connection&& other) noexcept;
    Connection& operator=(Connection&& other) noexcept;

    ~Connection();

    static Connection open(std::string_view dsn);

    PGconn* get() const noexcept;

    Result exec(
        std::string_view sql,
        std::initializer_list<ExecStatusType> expected
    );

    Result exec_command(std::string_view sql);
    Result exec_query(std::string_view sql);

    Result exec_params(
        std::string_view sql,
        const std::vector<std::string>& params,
        std::initializer_list<ExecStatusType> expected
    );

  private:
    void close() noexcept;

  private:
    PGconn* connection_ = nullptr;
};

[[noreturn]] void throw_postgres_error(
    PGconn* connection,
    std::string_view context
);

void require_result_status(
    PGconn* connection,
    PGresult* result,
    std::string_view context,
    std::initializer_list<ExecStatusType> expected
);

} // namespace mt::backends::postgres::detail
