#pragma once

#include "mt_database.hpp"
#include "mt_errors.hpp"
#include "mt_types.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------
// mt_transaction.hpp
//
// Transaction state, validation, and retry provider.
// -----------------------------------------------------------------------------

namespace mt
{

struct DocumentId
{
    CollectionId collection = 0;
    Key key;

    friend bool operator==(
        const DocumentId&,
        const DocumentId&
    ) = default;
};

struct DocumentIdHash
{
    std::size_t operator()(const DocumentId& id) const noexcept
    {
        auto h1 = std::hash<CollectionId>{}(id.collection);
        auto h2 = std::hash<std::string>{}(id.key);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
};

struct ReadRecord
{
    DocumentId id;
    Version observed_version = 0;
    Hash observed_hash;
};

struct PredicateObservedRow
{
    DocumentId id;
    Version observed_version = 0;
    Hash observed_hash;

    friend bool operator==(
        const PredicateObservedRow&,
        const PredicateObservedRow&
    ) = default;
};

struct PredicateReadRecord
{
    CollectionId collection = 0;
    QuerySpec query;
    ListOptions list_options;
    bool is_list = false;
    std::vector<PredicateObservedRow> observed_rows;
};

using ReadSet = std::unordered_map<DocumentId, ReadRecord, DocumentIdHash>;
using WriteSet = std::unordered_map<DocumentId, WriteEnvelope, DocumentIdHash>;
using PredicateReadSet = std::vector<PredicateReadRecord>;

class Transaction
{
  public:
    Transaction(Transaction&& other) noexcept
        : db_(other.db_),
          session_(std::move(other.session_)),
          tx_id_(std::move(other.tx_id_)),
          start_version_(other.start_version_),
          read_set_(std::move(other.read_set_)),
          write_set_(std::move(other.write_set_)),
          predicate_read_set_(std::move(other.predicate_read_set_)),
          finished_(other.finished_)
    {
        other.finished_ = true;
    }

    Transaction& operator=(Transaction&& other) noexcept
    {
        if (this == &other)
        {
            return *this;
        }

        rollback_if_open();

        db_ = other.db_;
        session_ = std::move(other.session_);
        tx_id_ = std::move(other.tx_id_);
        start_version_ = other.start_version_;
        read_set_ = std::move(other.read_set_);
        write_set_ = std::move(other.write_set_);
        predicate_read_set_ = std::move(other.predicate_read_set_);
        finished_ = other.finished_;

        other.finished_ = true;
        return *this;
    }

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;

    ~Transaction()
    {
        rollback_if_open();
    }

    Version start_version() const noexcept
    {
        return start_version_;
    }

    bool is_open() const noexcept
    {
        return !finished_;
    }

    void commit()
    {
        ensure_open();

        try
        {
            session_->lock_clock_and_read();

            validate_read_set();
            validate_write_set();
            validate_predicate_read_set();

            auto commit_version = session_->increment_clock_and_return();

            for (const auto& [_, write] : write_set_)
            {
                session_->insert_history(write.collection, write, commit_version);
                session_->upsert_current(write.collection, write, commit_version);
            }

            session_->unregister_active_transaction(tx_id_);
            session_->commit_backend_transaction();
            finished_ = true;
        }
        catch (...)
        {
            try
            {
                session_->rollback_backend_transaction();
            }
            catch (...)
            {
            }

            try
            {
                session_->unregister_active_transaction(tx_id_);
            }
            catch (...)
            {
            }

            finished_ = true;
            throw;
        }
    }

    void rollback()
    {
        ensure_open();
        rollback_if_open();
    }

  private:
    friend class TransactionProvider;

    template <class Row, class Mapping> friend class Table;

    Transaction(
        Database& db,
        std::unique_ptr<IBackendSession> session,
        TxId tx_id,
        Version start_version
    )
        : db_(&db),
          session_(std::move(session)),
          tx_id_(std::move(tx_id)),
          start_version_(start_version)
    {
    }

    void ensure_open() const
    {
        if (finished_)
        {
            throw TransactionClosed("transaction is already closed");
        }
    }

    void rollback_if_open() noexcept
    {
        if (finished_ || !session_)
        {
            return;
        }

        try
        {
            session_->rollback_backend_transaction();
        }
        catch (...)
        {
        }

        try
        {
            session_->unregister_active_transaction(tx_id_);
        }
        catch (...)
        {
        }

        finished_ = true;
    }

    std::optional<DocumentEnvelope> get_document(
        CollectionId collection,
        std::string_view key
    )
    {
        ensure_open();

        DocumentId id{collection, std::string(key)};

        auto write_it = write_set_.find(id);
        if (write_it != write_set_.end())
        {
            const auto& write = write_it->second;
            if (write.kind == WriteKind::Delete)
            {
                return std::nullopt;
            }

            return DocumentEnvelope{
                .collection = collection,
                .key = write.key,
                .version = start_version_,
                .deleted = false,
                .value_hash = write.value_hash,
                .value = write.value
            };
        }

        auto doc = session_->read_snapshot(collection, key, start_version_);

        ReadRecord record;
        record.id = id;

        if (doc && !doc->deleted)
        {
            record.observed_version = doc->version;
            record.observed_hash = doc->value_hash;
        }
        else if (doc && doc->deleted)
        {
            record.observed_version = doc->version;
            record.observed_hash = doc->value_hash;
            doc = std::nullopt;
        }
        else
        {
            record.observed_version = 0;
        }

        read_set_[id] = std::move(record);
        return doc;
    }

    QueryResultEnvelope list_documents(
        CollectionId collection,
        const ListOptions& options
    )
    {
        ensure_open();

        auto result = session_->list_snapshot(collection, options, start_version_);
        record_predicate_read(collection, QuerySpec{}, options, true, result);
        return overlay_writes_for_collection(collection, std::move(result));
    }

    QueryResultEnvelope query_documents(
        CollectionId collection,
        const QuerySpec& query
    )
    {
        ensure_open();

        auto result = session_->query_snapshot(collection, query, start_version_);
        record_predicate_read(collection, query, ListOptions{}, false, result);
        return overlay_writes_for_collection(collection, std::move(result));
    }

    void put_document(
        CollectionId collection,
        Key key,
        Json value
    )
    {
        ensure_open();

        auto value_hash = hash_json(value);
        DocumentId id{collection, key};

        write_set_[id] = WriteEnvelope{
            .collection = collection,
            .key = std::move(key),
            .kind = WriteKind::Put,
            .value = std::move(value),
            .value_hash = std::move(value_hash)
        };
    }

    void erase_document(
        CollectionId collection,
        std::string_view key
    )
    {
        ensure_open();

        DocumentId id{collection, std::string(key)};

        write_set_[id] = WriteEnvelope{
            .collection = collection,
            .key = std::string(key),
            .kind = WriteKind::Delete,
            .value = Json{},
            .value_hash = Hash{{0}}
        };
    }

    void record_predicate_read(
        CollectionId collection,
        QuerySpec query,
        ListOptions options,
        bool is_list,
        const QueryResultEnvelope& result
    )
    {
        PredicateReadRecord record;
        record.collection = collection;
        record.query = std::move(query);
        record.list_options = std::move(options);
        record.is_list = is_list;

        record.observed_rows.reserve(result.rows.size());

        for (const auto& row : result.rows)
        {
            if (row.deleted)
            {
                continue;
            }

            record.observed_rows.push_back(
                PredicateObservedRow{
                    .id = DocumentId{row.collection, row.key},
                    .observed_version = row.version,
                    .observed_hash = row.value_hash
                }
            );
        }

        std::sort(
            record.observed_rows.begin(), record.observed_rows.end(),
            [](const auto& a, const auto& b)
            {
                if (a.id.collection != b.id.collection)
                {
                    return a.id.collection < b.id.collection;
                }
                return a.id.key < b.id.key;
            }
        );

        predicate_read_set_.push_back(std::move(record));
    }

    QueryResultEnvelope overlay_writes_for_collection(
        CollectionId collection,
        QueryResultEnvelope result
    )
    {
        // Conservative first version: returned query/list results are the snapshot
        // rows. Local uncommitted writes are visible to point reads and commits.
        // A production version should evaluate pending writes against QuerySpec and
        // merge/delete them here so read-your-writes also applies to queries.
        (void)collection;
        return result;
    }

    void validate_read_set()
    {
        for (const auto& [id, read] : read_set_)
        {
            auto current = session_->read_current_metadata(id.collection, id.key);

            if (read.observed_version == 0)
            {
                if (current && current->version > start_version_)
                {
                    throw TransactionConflict(
                        "read conflict: key appeared after transaction start"
                    );
                }
                continue;
            }

            if (!current || current->version != read.observed_version)
            {
                throw TransactionConflict("read conflict: key changed after read");
            }
        }
    }

    void validate_write_set()
    {
        for (const auto& [id, write] : write_set_)
        {
            if (read_set_.contains(id))
            {
                continue;
            }

            auto current = session_->read_current_metadata(id.collection, id.key);
            if (current && current->version > start_version_)
            {
                throw TransactionConflict("write conflict: key changed after transaction start");
            }
        }
    }

    static std::vector<PredicateObservedRow>
    metadata_to_observed(const QueryMetadataResult& metadata)
    {
        std::vector<PredicateObservedRow> rows;
        rows.reserve(metadata.rows.size());

        for (const auto& row : metadata.rows)
        {
            if (row.deleted)
            {
                continue;
            }

            rows.push_back(
                PredicateObservedRow{
                    .id = DocumentId{row.collection, row.key},
                    .observed_version = row.version,
                    .observed_hash = row.value_hash
                }
            );
        }

        std::sort(
            rows.begin(), rows.end(),
            [](const auto& a, const auto& b)
            {
                if (a.id.collection != b.id.collection)
                {
                    return a.id.collection < b.id.collection;
                }
                return a.id.key < b.id.key;
            }
        );

        return rows;
    }

    void validate_predicate_read_set()
    {
        for (const auto& record : predicate_read_set_)
        {
            QueryMetadataResult current;

            if (record.is_list)
            {
                current = session_->list_current_metadata(record.collection, record.list_options);
            }
            else
            {
                current = session_->query_current_metadata(record.collection, record.query);
            }

            auto current_rows = metadata_to_observed(current);

            if (current_rows != record.observed_rows)
            {
                throw TransactionConflict("predicate conflict: result set changed");
            }
        }
    }

  private:
    Database* db_ = nullptr;
    std::unique_ptr<IBackendSession> session_;
    TxId tx_id_;
    Version start_version_ = 0;

    ReadSet read_set_;
    WriteSet write_set_;
    PredicateReadSet predicate_read_set_;

    bool finished_ = false;
};

struct RetryPolicy
{
    std::size_t max_attempts = 3;
    std::chrono::milliseconds base_delay{5};

    void backoff(std::size_t) const
    {
        // Hook for callers. Core library intentionally does not sleep by default.
    }
};

class TransactionProvider
{
  public:
    explicit TransactionProvider(Database& db)
        : db_(&db)
    {
    }

    Transaction begin()
    {
        auto session = db_->backend().open_session();
        session->begin_backend_transaction();

        try
        {
            auto start_version = session->read_clock();
            auto tx_id = session->create_transaction_id();
            session->register_active_transaction(tx_id, start_version);

            return Transaction(*db_, std::move(session), std::move(tx_id), start_version);
        }
        catch (...)
        {
            session->rollback_backend_transaction();
            throw;
        }
    }

    template <class Fn> decltype(auto) run(Fn&& fn)
    {
        auto tx = begin();

        try
        {
            if constexpr (std::is_void_v<std::invoke_result_t<Fn, Transaction&>>)
            {
                std::forward<Fn>(fn)(tx);
                tx.commit();
                return;
            }
            else
            {
                auto result = std::forward<Fn>(fn)(tx);
                tx.commit();
                return result;
            }
        }
        catch (...)
        {
            if (tx.is_open())
            {
                tx.rollback();
            }
            throw;
        }
    }

    template <class Fn>
    decltype(auto) retry(
        RetryPolicy policy,
        Fn& fn
    )
    {
        for (std::size_t attempt = 0; attempt < policy.max_attempts; ++attempt)
        {
            try
            {
                return run(fn);
            }
            catch (const TransactionConflict&)
            {
                if (attempt + 1 == policy.max_attempts)
                {
                    throw;
                }
                policy.backoff(attempt);
            }
        }

        throw TransactionConflict("retry attempts exhausted");
    }

  private:
    Database* db_ = nullptr;
};

} // namespace mt
