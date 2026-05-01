#include "postgres_connection.hpp"

#include <algorithm>
#include <utility>

namespace mt::backends::postgres::detail
{

namespace
{
std::string postgres_error_message(PGconn* connection)
{
    if (!connection)
    {
        return {};
    }

    auto message = std::string(PQerrorMessage(connection));
    while (!message.empty() && (message.back() == '\n' || message.back() == '\r'))
    {
        message.pop_back();
    }
    return message;
}

} // namespace

Result::Result(PGresult* result) noexcept
    : result_(result)
{
}

Result::Result(Result&& other) noexcept
    : result_(
          std::exchange(
              other.result_,
              nullptr
          )
      )
{
}

Result& Result::operator=(Result&& other) noexcept
{
    if (this != &other)
    {
        clear();
        result_ = std::exchange(other.result_, nullptr);
    }
    return *this;
}

Result::~Result()
{
    clear();
}

PGresult* Result::get() const noexcept
{
    return result_;
}

ExecStatusType Result::status() const noexcept
{
    return PQresultStatus(result_);
}

int Result::rows() const noexcept
{
    return PQntuples(result_);
}

int Result::columns() const noexcept
{
    return PQnfields(result_);
}

bool Result::is_null(
    int row,
    int column
) const
{
    return PQgetisnull(result_, row, column) == 1;
}

std::string_view Result::value(
    int row,
    int column
) const
{
    return std::string_view(PQgetvalue(result_, row, column), PQgetlength(result_, row, column));
}

void Result::clear() noexcept
{
    if (result_)
    {
        PQclear(result_);
        result_ = nullptr;
    }
}

Connection::Connection(PGconn* connection) noexcept
    : connection_(connection)
{
}

Connection::Connection(Connection&& other) noexcept
    : connection_(
          std::exchange(
              other.connection_,
              nullptr
          )
      )
{
}

Connection& Connection::operator=(Connection&& other) noexcept
{
    if (this != &other)
    {
        close();
        connection_ = std::exchange(other.connection_, nullptr);
    }
    return *this;
}

Connection::~Connection()
{
    close();
}

Connection Connection::open(std::string_view dsn)
{
    auto dsn_text = std::string(dsn);
    auto* connection = PQconnectdb(dsn_text.c_str());
    if (!connection)
    {
        throw BackendError("open postgres connection: libpq returned null connection");
    }

    if (PQstatus(connection) != CONNECTION_OK)
    {
        auto message = std::string("open postgres connection");
        auto detail = postgres_error_message(connection);
        if (!detail.empty())
        {
            message += ": ";
            message += detail;
        }
        PQfinish(connection);
        throw BackendError(message);
    }

    return Connection(connection);
}

PGconn* Connection::get() const noexcept
{
    return connection_;
}

Result Connection::exec(
    std::string_view sql,
    std::initializer_list<ExecStatusType> expected
)
{
    auto sql_text = std::string(sql);
    auto* result = PQexec(connection_, sql_text.c_str());
    require_result_status(connection_, result, sql, expected);
    return Result(result);
}

Result Connection::exec_command(std::string_view sql)
{
    return exec(sql, {PGRES_COMMAND_OK});
}

Result Connection::exec_query(std::string_view sql)
{
    return exec(sql, {PGRES_TUPLES_OK});
}

Result Connection::exec_params(
    std::string_view sql,
    const std::vector<std::string>& params,
    std::initializer_list<ExecStatusType> expected
)
{
    auto sql_text = std::string(sql);
    auto values = std::vector<const char*>{};
    values.reserve(params.size());
    for (const auto& param : params)
    {
        values.push_back(param.c_str());
    }

    auto* result = PQexecParams(
        connection_, sql_text.c_str(), static_cast<int>(values.size()), nullptr, values.data(),
        nullptr, nullptr, 0
    );
    require_result_status(connection_, result, sql, expected);
    return Result(result);
}

void Connection::close() noexcept
{
    if (connection_)
    {
        PQfinish(connection_);
        connection_ = nullptr;
    }
}

[[noreturn]] void throw_postgres_error(
    PGconn* connection,
    std::string_view context
)
{
    auto message = std::string(context);
    auto detail = postgres_error_message(connection);
    if (!detail.empty())
    {
        message += ": ";
        message += detail;
    }
    throw BackendError(message);
}

void require_result_status(
    PGconn* connection,
    PGresult* result,
    std::string_view context,
    std::initializer_list<ExecStatusType> expected
)
{
    if (!result)
    {
        throw_postgres_error(connection, context);
    }

    auto status = PQresultStatus(result);
    if (std::find(expected.begin(), expected.end(), status) == expected.end())
    {
        PQclear(result);
        throw_postgres_error(connection, context);
    }
}

} // namespace mt::backends::postgres::detail
