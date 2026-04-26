#pragma once

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------
// mt_core.hpp
//
// Backend-agnostic micro-transaction core library.
//
// This file intentionally contains no PostgreSQL types and no SQL. Concrete
// storage engines implement IDatabaseBackend and IBackendSession.
// -----------------------------------------------------------------------------

namespace mt {

// -----------------------------------------------------------------------------
// Basic JSON placeholder
// -----------------------------------------------------------------------------
// Replace this with nlohmann::json, boost::json::value, simdjson DOM, or your
// internal JSON value type. The rest of the library depends only on Mapping
// implementations producing and consuming mt::Json.

class Json {
public:
    Json() = default;

    // Placeholder constructors for examples. Replace in production.
    Json(std::nullptr_t) {}

    friend bool operator==(const Json&, const Json&) = default;
};

// -----------------------------------------------------------------------------
// IDs and hashes
// -----------------------------------------------------------------------------

using CollectionId = std::uint64_t;
using Version = std::uint64_t;
using TxId = std::string;
using Key = std::string;

struct Hash {
    std::vector<std::uint8_t> bytes;

    friend bool operator==(const Hash&, const Hash&) = default;
};

inline TxId generate_tx_id() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};

    auto next = [] {
        return rng();
    };

    return std::to_string(next()) + "-" + std::to_string(next());
}

// Default hash placeholder. A production implementation should use a stable
// cryptographic or strong non-cryptographic hash over canonical JSON bytes.
inline Hash hash_json(const Json&) {
    return Hash{{0}};
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class TransactionConflict : public Error {
public:
    using Error::Error;
};

class TransactionClosed : public Error {
public:
    using Error::Error;
};

class DocumentNotFound : public Error {
public:
    using Error::Error;
};

class MappingError : public Error {
public:
    using Error::Error;
};

class BackendError : public Error {
public:
    using Error::Error;
};

// -----------------------------------------------------------------------------
// Query and index model
// -----------------------------------------------------------------------------

struct ListOptions {
    std::optional<std::size_t> limit;
    std::optional<Key> after_key;
};

enum class QueryOp {
    JsonEquals,
    JsonContains,
    KeyPrefix
};

struct QueryPredicate {
    QueryOp op{};
    std::string path;
    Json value;
    std::string text;
};

struct QuerySpec {
    std::vector<QueryPredicate> predicates;
    std::optional<std::size_t> limit;
    std::optional<Key> after_key;
    bool order_by_key = true;

    static QuerySpec where_json_eq(std::string path, Json value) {
        QuerySpec q;
        q.predicates.push_back(QueryPredicate{
            .op = QueryOp::JsonEquals,
            .path = std::move(path),
            .value = std::move(value)
        });
        return q;
    }

    static QuerySpec key_prefix(std::string prefix) {
        QuerySpec q;
        q.predicates.push_back(QueryPredicate{
            .op = QueryOp::KeyPrefix,
            .text = std::move(prefix)
        });
        return q;
    }
};

struct IndexSpec {
    std::string name;
    std::string json_path;
    bool unique = false;

    static IndexSpec json_path_index(std::string name, std::string path) {
        return IndexSpec{
            .name = std::move(name),
            .json_path = std::move(path),
            .unique = false
        };
    }

    IndexSpec& make_unique() {
        unique = true;
        return *this;
    }
};

struct Migration {
    int from_version = 0;
    int to_version = 0;
    std::function<void(Json&)> transform;
};

// -----------------------------------------------------------------------------
// Collection descriptors
// -----------------------------------------------------------------------------

struct CollectionSpec {
    std::string logical_name;
    std::vector<IndexSpec> indexes;
    int schema_version = 1;
    std::vector<Migration> migrations;
};

struct CollectionDescriptor {
    CollectionId id = 0;
    std::string logical_name;
    int schema_version = 1;
};

struct BootstrapSpec {
    int metadata_schema_version = 1;
};

// -----------------------------------------------------------------------------
// Backend document envelopes
// -----------------------------------------------------------------------------

struct DocumentEnvelope {
    CollectionId collection = 0;
    Key key;
    Version version = 0;
    bool deleted = false;
    Hash value_hash;
    Json value;
};

struct DocumentMetadata {
    CollectionId collection = 0;
    Key key;
    Version version = 0;
    bool deleted = false;
    Hash value_hash;
};

struct QueryResultEnvelope {
    std::vector<DocumentEnvelope> rows;
};

struct QueryMetadataResult {
    std::vector<DocumentMetadata> rows;
};

enum class WriteKind {
    Put,
    Delete
};

struct WriteEnvelope {
    CollectionId collection = 0;
    Key key;
    WriteKind kind = WriteKind::Put;
    Json value;
    Hash value_hash;
};

// -----------------------------------------------------------------------------
// Backend interfaces
// -----------------------------------------------------------------------------

class IBackendSession {
public:
    virtual ~IBackendSession() = default;

    virtual void begin_backend_transaction() = 0;
    virtual void commit_backend_transaction() = 0;
    virtual void rollback_backend_transaction() noexcept = 0;

    virtual Version read_clock() = 0;
    virtual Version lock_clock_and_read() = 0;
    virtual Version increment_clock_and_return() = 0;

    virtual void register_active_transaction(TxId tx_id, Version start_version) = 0;
    virtual void unregister_active_transaction(TxId tx_id) noexcept = 0;

    virtual std::optional<DocumentEnvelope> read_snapshot(
        CollectionId collection,
        std::string_view key,
        Version version) = 0;

    virtual std::optional<DocumentMetadata> read_current_metadata(
        CollectionId collection,
        std::string_view key) = 0;

    virtual QueryResultEnvelope query_snapshot(
        CollectionId collection,
        const QuerySpec& query,
        Version version) = 0;

    virtual QueryMetadataResult query_current_metadata(
        CollectionId collection,
        const QuerySpec& query) = 0;

    virtual QueryResultEnvelope list_snapshot(
        CollectionId collection,
        const ListOptions& options,
        Version version) = 0;

    virtual QueryMetadataResult list_current_metadata(
        CollectionId collection,
        const ListOptions& options) = 0;

    virtual void insert_history(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version) = 0;

    virtual void upsert_current(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version) = 0;
};

class IDatabaseBackend {
public:
    virtual ~IDatabaseBackend() = default;

    virtual std::unique_ptr<IBackendSession> open_session() = 0;

    virtual void bootstrap(const BootstrapSpec& spec) = 0;

    virtual CollectionDescriptor ensure_collection(const CollectionSpec& spec) = 0;

    virtual CollectionDescriptor get_collection(std::string_view logical_name) = 0;
};

// -----------------------------------------------------------------------------
// Metadata cache
// -----------------------------------------------------------------------------

class MetadataCache {
public:
    std::optional<CollectionDescriptor> find(std::string_view logical_name) const {
        auto it = by_name_.find(std::string(logical_name));
        if (it == by_name_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    void put(CollectionDescriptor descriptor) {
        by_name_[descriptor.logical_name] = std::move(descriptor);
    }

private:
    std::unordered_map<std::string, CollectionDescriptor> by_name_;
};

// -----------------------------------------------------------------------------
// Database
// -----------------------------------------------------------------------------

class TransactionProvider;
class TableProvider;
class Transaction;

template <class Row, class Mapping>
class Table;

class Database {
public:
    explicit Database(std::shared_ptr<IDatabaseBackend> backend)
        : backend_(std::move(backend)) {
        if (!backend_) {
            throw BackendError("Database requires a non-null backend");
        }
        backend_->bootstrap(BootstrapSpec{});
    }

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

private:
    friend class TransactionProvider;
    friend class TableProvider;

    template <class Row, class Mapping>
    friend class Table;

    IDatabaseBackend& backend() {
        return *backend_;
    }

    const IDatabaseBackend& backend() const {
        return *backend_;
    }

    MetadataCache& metadata() {
        return metadata_;
    }

private:
    std::shared_ptr<IDatabaseBackend> backend_;
    MetadataCache metadata_;
};

// -----------------------------------------------------------------------------
// Mapping concept
// -----------------------------------------------------------------------------

template <class Mapping, class Row>
concept RowMapping = requires(Row row, Json json) {
    { Mapping::table_name } -> std::convertible_to<std::string_view>;
    { Mapping::key(row) } -> std::convertible_to<std::string>;
    { Mapping::to_json(row) } -> std::same_as<Json>;
    { Mapping::from_json(json) } -> std::same_as<Row>;
};

template <class Mapping>
std::vector<IndexSpec> mapping_indexes_or_empty() {
    if constexpr (requires { Mapping::indexes(); }) {
        return Mapping::indexes();
    } else {
        return {};
    }
}

template <class Mapping>
std::vector<Migration> mapping_migrations_or_empty() {
    if constexpr (requires { Mapping::migrations(); }) {
        return Mapping::migrations();
    } else {
        return {};
    }
}

template <class Mapping>
int mapping_schema_version_or_default() {
    if constexpr (requires { Mapping::schema_version; }) {
        return Mapping::schema_version;
    } else {
        return 1;
    }
}

// -----------------------------------------------------------------------------
// Read/write tracking
// -----------------------------------------------------------------------------

struct DocumentId {
    CollectionId collection = 0;
    Key key;

    friend bool operator==(const DocumentId&, const DocumentId&) = default;
};

struct DocumentIdHash {
    std::size_t operator()(const DocumentId& id) const noexcept {
        auto h1 = std::hash<CollectionId>{}(id.collection);
        auto h2 = std::hash<std::string>{}(id.key);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
};

struct ReadRecord {
    DocumentId id;
    Version observed_version = 0;
    Hash observed_hash;
};

struct PredicateObservedRow {
    DocumentId id;
    Version observed_version = 0;
    Hash observed_hash;

    friend bool operator==(const PredicateObservedRow&, const PredicateObservedRow&) = default;
};

struct PredicateReadRecord {
    CollectionId collection = 0;
    QuerySpec query;
    ListOptions list_options;
    bool is_list = false;
    std::vector<PredicateObservedRow> observed_rows;
};

using ReadSet = std::unordered_map<DocumentId, ReadRecord, DocumentIdHash>;
using WriteSet = std::unordered_map<DocumentId, WriteEnvelope, DocumentIdHash>;
using PredicateReadSet = std::vector<PredicateReadRecord>;

// -----------------------------------------------------------------------------
// Transaction
// -----------------------------------------------------------------------------

class Transaction {
public:
    Transaction(Transaction&& other) noexcept
        : db_(other.db_),
          session_(std::move(other.session_)),
          tx_id_(std::move(other.tx_id_)),
          start_version_(other.start_version_),
          read_set_(std::move(other.read_set_)),
          write_set_(std::move(other.write_set_)),
          predicate_read_set_(std::move(other.predicate_read_set_)),
          finished_(other.finished_) {
        other.finished_ = true;
    }

    Transaction& operator=(Transaction&& other) noexcept {
        if (this == &other) {
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

    ~Transaction() {
        rollback_if_open();
    }

    Version start_version() const noexcept {
        return start_version_;
    }

    bool is_open() const noexcept {
        return !finished_;
    }

    void commit() {
        ensure_open();

        try {
            session_->lock_clock_and_read();

            validate_read_set();
            validate_write_set();
            validate_predicate_read_set();

            auto commit_version = session_->increment_clock_and_return();

            for (const auto& [_, write] : write_set_) {
                session_->insert_history(write.collection, write, commit_version);
                session_->upsert_current(write.collection, write, commit_version);
            }

            session_->unregister_active_transaction(tx_id_);
            session_->commit_backend_transaction();
            finished_ = true;
        } catch (...) {
            try {
                session_->rollback_backend_transaction();
            } catch (...) {
            }

            try {
                session_->unregister_active_transaction(tx_id_);
            } catch (...) {
            }

            finished_ = true;
            throw;
        }
    }

    void rollback() {
        ensure_open();
        rollback_if_open();
    }

private:
    friend class TransactionProvider;

    template <class Row, class Mapping>
    friend class Table;

    Transaction(
        Database& db,
        std::unique_ptr<IBackendSession> session,
        TxId tx_id,
        Version start_version)
        : db_(&db),
          session_(std::move(session)),
          tx_id_(std::move(tx_id)),
          start_version_(start_version) {}

    void ensure_open() const {
        if (finished_) {
            throw TransactionClosed("transaction is already closed");
        }
    }

    void rollback_if_open() noexcept {
        if (finished_ || !session_) {
            return;
        }

        try {
            session_->rollback_backend_transaction();
        } catch (...) {
        }

        try {
            session_->unregister_active_transaction(tx_id_);
        } catch (...) {
        }

        finished_ = true;
    }

    std::optional<DocumentEnvelope> get_document(
        CollectionId collection,
        std::string_view key) {
        ensure_open();

        DocumentId id{collection, std::string(key)};

        auto write_it = write_set_.find(id);
        if (write_it != write_set_.end()) {
            const auto& write = write_it->second;
            if (write.kind == WriteKind::Delete) {
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

        if (doc && !doc->deleted) {
            record.observed_version = doc->version;
            record.observed_hash = doc->value_hash;
        } else if (doc && doc->deleted) {
            record.observed_version = doc->version;
            record.observed_hash = doc->value_hash;
            doc = std::nullopt;
        } else {
            record.observed_version = 0;
        }

        read_set_[id] = std::move(record);
        return doc;
    }

    QueryResultEnvelope list_documents(
        CollectionId collection,
        const ListOptions& options) {
        ensure_open();

        auto result = session_->list_snapshot(collection, options, start_version_);
        record_predicate_read(collection, QuerySpec{}, options, true, result);
        return overlay_writes_for_collection(collection, std::move(result));
    }

    QueryResultEnvelope query_documents(
        CollectionId collection,
        const QuerySpec& query) {
        ensure_open();

        auto result = session_->query_snapshot(collection, query, start_version_);
        record_predicate_read(collection, query, ListOptions{}, false, result);
        return overlay_writes_for_collection(collection, std::move(result));
    }

    void put_document(CollectionId collection, Key key, Json value) {
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

    void erase_document(CollectionId collection, std::string_view key) {
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
        const QueryResultEnvelope& result) {
        PredicateReadRecord record;
        record.collection = collection;
        record.query = std::move(query);
        record.list_options = std::move(options);
        record.is_list = is_list;

        record.observed_rows.reserve(result.rows.size());

        for (const auto& row : result.rows) {
            if (row.deleted) {
                continue;
            }

            record.observed_rows.push_back(PredicateObservedRow{
                .id = DocumentId{row.collection, row.key},
                .observed_version = row.version,
                .observed_hash = row.value_hash
            });
        }

        std::sort(
            record.observed_rows.begin(),
            record.observed_rows.end(),
            [](const auto& a, const auto& b) {
                if (a.id.collection != b.id.collection) {
                    return a.id.collection < b.id.collection;
                }
                return a.id.key < b.id.key;
            });

        predicate_read_set_.push_back(std::move(record));
    }

    QueryResultEnvelope overlay_writes_for_collection(
        CollectionId collection,
        QueryResultEnvelope result) {
        // Conservative first version: returned query/list results are the snapshot
        // rows. Local uncommitted writes are visible to point reads and commits.
        // A production version should evaluate pending writes against QuerySpec and
        // merge/delete them here so read-your-writes also applies to queries.
        (void)collection;
        return result;
    }

    void validate_read_set() {
        for (const auto& [id, read] : read_set_) {
            auto current = session_->read_current_metadata(id.collection, id.key);

            if (read.observed_version == 0) {
                if (current && current->version > start_version_) {
                    throw TransactionConflict("read conflict: key appeared after transaction start");
                }
                continue;
            }

            if (!current || current->version != read.observed_version) {
                throw TransactionConflict("read conflict: key changed after read");
            }
        }
    }

    void validate_write_set() {
        for (const auto& [id, write] : write_set_) {
            if (read_set_.contains(id)) {
                continue;
            }

            auto current = session_->read_current_metadata(id.collection, id.key);
            if (current && current->version > start_version_) {
                throw TransactionConflict("write conflict: key changed after transaction start");
            }
        }
    }

    static std::vector<PredicateObservedRow> metadata_to_observed(
        const QueryMetadataResult& metadata) {
        std::vector<PredicateObservedRow> rows;
        rows.reserve(metadata.rows.size());

        for (const auto& row : metadata.rows) {
            if (row.deleted) {
                continue;
            }

            rows.push_back(PredicateObservedRow{
                .id = DocumentId{row.collection, row.key},
                .observed_version = row.version,
                .observed_hash = row.value_hash
            });
        }

        std::sort(
            rows.begin(),
            rows.end(),
            [](const auto& a, const auto& b) {
                if (a.id.collection != b.id.collection) {
                    return a.id.collection < b.id.collection;
                }
                return a.id.key < b.id.key;
            });

        return rows;
    }

    void validate_predicate_read_set() {
        for (const auto& record : predicate_read_set_) {
            QueryMetadataResult current;

            if (record.is_list) {
                current = session_->list_current_metadata(
                    record.collection,
                    record.list_options);
            } else {
                current = session_->query_current_metadata(
                    record.collection,
                    record.query);
            }

            auto current_rows = metadata_to_observed(current);

            if (current_rows != record.observed_rows) {
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

// -----------------------------------------------------------------------------
// Retry policy
// -----------------------------------------------------------------------------

struct RetryPolicy {
    std::size_t max_attempts = 3;
    std::chrono::milliseconds base_delay{5};

    void backoff(std::size_t) const {
        // Hook for callers. Core library intentionally does not sleep by default.
    }
};

// -----------------------------------------------------------------------------
// TransactionProvider
// -----------------------------------------------------------------------------

class TransactionProvider {
public:
    explicit TransactionProvider(Database& db)
        : db_(&db) {}

    Transaction begin() {
        auto session = db_->backend().open_session();
        session->begin_backend_transaction();

        try {
            auto start_version = session->read_clock();
            auto tx_id = generate_tx_id();
            session->register_active_transaction(tx_id, start_version);

            return Transaction(
                *db_,
                std::move(session),
                std::move(tx_id),
                start_version);
        } catch (...) {
            session->rollback_backend_transaction();
            throw;
        }
    }

    template <class Fn>
    decltype(auto) run(Fn&& fn) {
        auto tx = begin();

        try {
            if constexpr (std::is_void_v<std::invoke_result_t<Fn, Transaction&>>) {
                std::forward<Fn>(fn)(tx);
                tx.commit();
                return;
            } else {
                auto result = std::forward<Fn>(fn)(tx);
                tx.commit();
                return result;
            }
        } catch (...) {
            if (tx.is_open()) {
                tx.rollback();
            }
            throw;
        }
    }

    template <class Fn>
    decltype(auto) retry(RetryPolicy policy, Fn& fn) {
        for (std::size_t attempt = 0; attempt < policy.max_attempts; ++attempt) {
            try {
                return run(fn);
            } catch (const TransactionConflict&) {
                if (attempt + 1 == policy.max_attempts) {
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

// -----------------------------------------------------------------------------
// TableProvider
// -----------------------------------------------------------------------------

class TableProvider {
public:
    explicit TableProvider(Database& db)
        : db_(&db) {}

    template <class Row, class Mapping>
    Table<Row, Mapping> table();

private:
    Database* db_ = nullptr;
};

// -----------------------------------------------------------------------------
// Table
// -----------------------------------------------------------------------------

template <class Row, class Mapping>
requires RowMapping<Mapping, Row>
class Table {
public:
    using row_type = Row;
    using mapping_type = Mapping;

    Table(Database& db, CollectionDescriptor descriptor)
        : db_(&db), descriptor_(std::move(descriptor)) {}

    const CollectionDescriptor& descriptor() const noexcept {
        return descriptor_;
    }

    std::optional<Row> get(std::string_view key) const {
        auto session = db_->backend().open_session();
        session->begin_backend_transaction();

        try {
            auto read_version = session->read_clock();
            auto doc = session->read_snapshot(descriptor_.id, key, read_version);
            session->commit_backend_transaction();

            if (!doc || doc->deleted) {
                return std::nullopt;
            }

            return Mapping::from_json(doc->value);
        } catch (...) {
            session->rollback_backend_transaction();
            throw;
        }
    }

    Row require(std::string_view key) const {
        auto row = get(key);
        if (!row) {
            throw DocumentNotFound("document not found");
        }
        return *std::move(row);
    }

    std::vector<Row> list(ListOptions options = {}) const {
        auto session = db_->backend().open_session();
        session->begin_backend_transaction();

        try {
            auto read_version = session->read_clock();
            auto result = session->list_snapshot(descriptor_.id, options, read_version);
            session->commit_backend_transaction();

            return decode_rows(result);
        } catch (...) {
            session->rollback_backend_transaction();
            throw;
        }
    }

    std::vector<Row> query(const QuerySpec& query) const {
        auto session = db_->backend().open_session();
        session->begin_backend_transaction();

        try {
            auto read_version = session->read_clock();
            auto result = session->query_snapshot(descriptor_.id, query, read_version);
            session->commit_backend_transaction();

            return decode_rows(result);
        } catch (...) {
            session->rollback_backend_transaction();
            throw;
        }
    }

    std::optional<Row> get(Transaction& tx, std::string_view key) const {
        auto doc = tx.get_document(descriptor_.id, key);
        if (!doc || doc->deleted) {
            return std::nullopt;
        }
        return Mapping::from_json(doc->value);
    }

    Row require(Transaction& tx, std::string_view key) const {
        auto row = get(tx, key);
        if (!row) {
            throw DocumentNotFound("document not found");
        }
        return *std::move(row);
    }

    std::vector<Row> list(Transaction& tx, ListOptions options = {}) const {
        auto result = tx.list_documents(descriptor_.id, options);
        return decode_rows(result);
    }

    std::vector<Row> query(Transaction& tx, const QuerySpec& query) const {
        auto result = tx.query_documents(descriptor_.id, query);
        return decode_rows(result);
    }

    void put(Transaction& tx, const Row& row) const {
        auto key = Mapping::key(row);
        auto json = Mapping::to_json(row);
        tx.put_document(descriptor_.id, std::move(key), std::move(json));
    }

    void erase(Transaction& tx, std::string_view key) const {
        tx.erase_document(descriptor_.id, key);
    }

private:
    static std::vector<Row> decode_rows(const QueryResultEnvelope& result) {
        std::vector<Row> rows;
        rows.reserve(result.rows.size());

        for (const auto& doc : result.rows) {
            if (!doc.deleted) {
                rows.push_back(Mapping::from_json(doc.value));
            }
        }

        return rows;
    }

private:
    Database* db_ = nullptr;
    CollectionDescriptor descriptor_;
};

template <class Row, class Mapping>
Table<Row, Mapping> TableProvider::table() {
    static_assert(RowMapping<Mapping, Row>,
        "Mapping must define table_name, key(row), to_json(row), and from_json(json)");

    auto table_name = std::string(Mapping::table_name);

    if (auto cached = db_->metadata().find(table_name)) {
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
