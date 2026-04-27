#pragma once

#include "mt/collection.hpp"
#include "mt/database.hpp"
#include "mt/errors.hpp"
#include "mt/query.hpp"
#include "mt/transaction.hpp"

#include <concepts>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------
// mt/table.hpp
//
// Mapping concept, table provider, and typed table facade.
// -----------------------------------------------------------------------------

namespace mt
{

template <class Mapping, class Row>
concept RowMapping = requires(Row row, Json json) {
    { Mapping::table_name } -> std::convertible_to<std::string_view>;
    { Mapping::key(row) } -> std::convertible_to<std::string>;
    { Mapping::to_json(row) } -> std::same_as<Json>;
    { Mapping::from_json(json) } -> std::same_as<Row>;
};

template <class Mapping> std::vector<IndexSpec> mapping_indexes_or_empty()
{
    if constexpr (requires { Mapping::indexes(); })
    {
        return Mapping::indexes();
    }
    else
    {
        return {};
    }
}

template <class Mapping> std::vector<Migration> mapping_migrations_or_empty()
{
    if constexpr (requires { Mapping::migrations(); })
    {
        return Mapping::migrations();
    }
    else
    {
        return {};
    }
}

template <class Mapping> int mapping_schema_version_or_default()
{
    if constexpr (requires { Mapping::schema_version; })
    {
        return Mapping::schema_version;
    }
    else
    {
        return 1;
    }
}

class TableProvider
{
  public:
    explicit TableProvider(Database& db)
        : db_(&db)
    {
    }

    template <
        class Row,
        class Mapping>
    Table<
        Row,
        Mapping>
    table();

  private:
    Database* db_ = nullptr;
};

template <class Row, class Mapping> class Table
{
    static_assert(
        RowMapping<
            Mapping,
            Row>,
        "Mapping must define table_name, key(row), to_json(row), and "
        "from_json(json)"
    );

  public:
    using row_type = Row;
    using mapping_type = Mapping;

    Table(
        Database& db,
        CollectionDescriptor descriptor
    )
        : db_(&db),
          descriptor_(std::move(descriptor))
    {
    }

    const CollectionDescriptor& descriptor() const noexcept
    {
        return descriptor_;
    }

    std::optional<Row> get(std::string_view key) const
    {
        auto session = db_->backend().open_session();
        session->begin_backend_transaction();

        try
        {
            auto read_version = session->read_clock();
            auto doc = session->read_snapshot(descriptor_.id, key, read_version);
            session->commit_backend_transaction();

            if (!doc || doc->deleted)
            {
                return std::nullopt;
            }

            return Mapping::from_json(doc->value);
        }
        catch (...)
        {
            session->rollback_backend_transaction();
            throw;
        }
    }

    Row require(std::string_view key) const
    {
        auto row = get(key);
        if (!row)
        {
            throw DocumentNotFound("document not found");
        }
        return *std::move(row);
    }

    std::vector<Row> list(ListOptions options = {}) const
    {
        auto session = db_->backend().open_session();
        session->begin_backend_transaction();

        try
        {
            auto read_version = session->read_clock();
            auto result = session->list_snapshot(descriptor_.id, options, read_version);
            session->commit_backend_transaction();

            return decode_rows(result);
        }
        catch (...)
        {
            session->rollback_backend_transaction();
            throw;
        }
    }

    std::vector<Row> query(const QuerySpec& query) const
    {
        auto session = db_->backend().open_session();
        session->begin_backend_transaction();

        try
        {
            auto read_version = session->read_clock();
            auto result = session->query_snapshot(descriptor_.id, query, read_version);
            session->commit_backend_transaction();

            return decode_rows(result);
        }
        catch (...)
        {
            session->rollback_backend_transaction();
            throw;
        }
    }

    std::optional<Row>
    get(Transaction& tx,
        std::string_view key) const
    {
        auto doc = tx.get_document(descriptor_.id, key);
        if (!doc || doc->deleted)
        {
            return std::nullopt;
        }
        return Mapping::from_json(doc->value);
    }

    Row require(
        Transaction& tx,
        std::string_view key
    ) const
    {
        auto row = get(tx, key);
        if (!row)
        {
            throw DocumentNotFound("document not found");
        }
        return *std::move(row);
    }

    std::vector<Row> list(
        Transaction& tx,
        ListOptions options = {}
    ) const
    {
        auto result = tx.list_documents(descriptor_.id, options);
        return decode_rows(result);
    }

    std::vector<Row> query(
        Transaction& tx,
        const QuerySpec& query
    ) const
    {
        auto result = tx.query_documents(descriptor_.id, query);
        return decode_rows(result);
    }

    void
    put(Transaction& tx,
        const Row& row) const
    {
        auto key = Mapping::key(row);
        auto json = Mapping::to_json(row);
        tx.put_document(descriptor_.id, std::move(key), std::move(json));
    }

    void erase(
        Transaction& tx,
        std::string_view key
    ) const
    {
        tx.erase_document(descriptor_.id, key);
    }

  private:
    static std::vector<Row> decode_rows(const QueryResultEnvelope& result)
    {
        std::vector<Row> rows;
        rows.reserve(result.rows.size());

        for (const auto& doc : result.rows)
        {
            if (!doc.deleted)
            {
                rows.push_back(Mapping::from_json(doc.value));
            }
        }

        return rows;
    }

  private:
    Database* db_ = nullptr;
    CollectionDescriptor descriptor_;
};

template <
    class Row,
    class Mapping>
Table<
    Row,
    Mapping>
TableProvider::table()
{
    static_assert(
        RowMapping<Mapping, Row>, "Mapping must define table_name, key(row), to_json(row), and "
                                  "from_json(json)"
    );

    auto table_name = std::string(Mapping::table_name);

    if (auto cached = db_->metadata().find(table_name))
    {
        return Table<Row, Mapping>(*db_, *cached);
    }

    CollectionSpec spec{
        .logical_name = table_name,
        .indexes = mapping_indexes_or_empty<Mapping>(),
        .schema_version = mapping_schema_version_or_default<Mapping>(),
        .migrations = mapping_migrations_or_empty<Mapping>()
    };

    auto descriptor = db_->backend().ensure_collection(spec);
    db_->metadata().put(descriptor);

    return Table<Row, Mapping>(*db_, std::move(descriptor));
}

} // namespace mt
