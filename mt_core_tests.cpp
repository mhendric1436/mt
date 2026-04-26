#include "mt_core.hpp"

#include <cassert>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <unordered_set>

// -----------------------------------------------------------------------------
// Minimal tests for mt_core.hpp
//
// These tests use a fake in-memory backend that implements the backend contracts
// expected by mt_core.hpp. It is intentionally small and deterministic.
//
// Build example:
//   c++ -std=c++20 -Wall -Wextra -pedantic mt_core_tests.cpp -o mt_core_tests
//
// Run:
//   ./mt_core_tests
// -----------------------------------------------------------------------------

namespace test_support {

// -----------------------------------------------------------------------------
// Test row and fake JSON model
// -----------------------------------------------------------------------------
// mt_core.hpp currently defines mt::Json as a placeholder. For real production
// tests, replace mt::Json in the core with your real JSON type. These tests only
// rely on equality and use out-of-band maps to represent row data.

struct User {
    std::string id;
    std::string email;
    std::string name;
    bool active = true;
    std::int64_t login_count = 0;

    friend bool operator==(const User&, const User&) = default;
};

// Since mt::Json is a placeholder, this fake codec stores objects in process
// memory and places an opaque handle into the Json value. In a production test,
// use nlohmann::json or your actual mt::Json implementation instead.
class FakeJsonRegistry {
public:
    static mt::Json encode_user(const User& user) {
        std::lock_guard lock(mutex_);
        const auto id = ++next_id_;
        users_[id] = user;
        json_ids_[address_key(mt::Json{})] = id;

        // mt::Json has no payload in the skeleton. Return a default object and
        // rely on encode/decode call ordering through latest_id_. This is a test
        // harness compromise until mt::Json is replaced with a real type.
        latest_id_ = id;
        return mt::Json{};
    }

    static User decode_latest_user() {
        std::lock_guard lock(mutex_);
        return users_.at(latest_id_);
    }

    static User decode_user_from_envelope_key(const std::string& key) {
        std::lock_guard lock(mutex_);
        return latest_by_key_.at(key);
    }

    static void remember_key_value(const std::string& key, const User& user) {
        std::lock_guard lock(mutex_);
        latest_by_key_[key] = user;
    }

private:
    static std::uintptr_t address_key(const mt::Json& value) {
        return reinterpret_cast<std::uintptr_t>(&value);
    }

    inline static std::mutex mutex_;
    inline static std::uint64_t next_id_ = 0;
    inline static std::uint64_t latest_id_ = 0;
    inline static std::map<std::uint64_t, User> users_;
    inline static std::map<std::uintptr_t, std::uint64_t> json_ids_;
    inline static std::map<std::string, User> latest_by_key_;
};

struct UserMapping {
    static constexpr std::string_view table_name = "users";
    static constexpr int schema_version = 1;

    static std::string key(const User& user) {
        return user.id;
    }

    static mt::Json to_json(const User& user) {
        FakeJsonRegistry::remember_key_value(user.id, user);
        return FakeJsonRegistry::encode_user(user);
    }

    static User from_json(const mt::Json&) {
        return FakeJsonRegistry::decode_latest_user();
    }

    static std::vector<mt::IndexSpec> indexes() {
        return {
            mt::IndexSpec::json_path_index("email", "$.email").make_unique(),
            mt::IndexSpec::json_path_index("active", "$.active")
        };
    }
};

// -----------------------------------------------------------------------------
// In-memory backend
// -----------------------------------------------------------------------------

struct MemoryVersion {
    mt::Version version = 0;
    bool deleted = false;
    mt::Json value;
    mt::Hash hash;
    std::string key;
};

struct MemoryCollection {
    mt::CollectionDescriptor descriptor;
    std::map<std::string, MemoryVersion> current;
    std::map<std::string, std::vector<MemoryVersion>> history;
};

struct MemoryState {
    std::mutex mutex;
    mt::Version clock = 0;
    mt::CollectionId next_collection_id = 1;
    std::map<std::string, mt::CollectionDescriptor> descriptors_by_name;
    std::map<mt::CollectionId, MemoryCollection> collections;
    std::unordered_set<mt::TxId> active_transactions;
    bool clock_locked = false;
};

class MemorySession final : public mt::IBackendSession {
public:
    explicit MemorySession(std::shared_ptr<MemoryState> state)
        : state_(std::move(state)) {}

    void begin_backend_transaction() override {
        in_backend_tx_ = true;
    }

    void commit_backend_transaction() override {
        require_backend_tx();
        release_clock_if_locked();
        in_backend_tx_ = false;
    }

    void rollback_backend_transaction() noexcept override {
        release_clock_if_locked();
        in_backend_tx_ = false;
    }

    mt::Version read_clock() override {
        std::lock_guard lock(state_->mutex);
        return state_->clock;
    }

    mt::Version lock_clock_and_read() override {
        std::lock_guard lock(state_->mutex);
        if (state_->clock_locked) {
            throw mt::BackendError("memory clock already locked");
        }
        state_->clock_locked = true;
        owns_clock_lock_ = true;
        return state_->clock;
    }

    mt::Version increment_clock_and_return() override {
        std::lock_guard lock(state_->mutex);
        if (!owns_clock_lock_) {
            throw mt::BackendError("clock must be locked before increment");
        }
        return ++state_->clock;
    }

    void register_active_transaction(mt::TxId tx_id, mt::Version) override {
        std::lock_guard lock(state_->mutex);
        active_tx_id_ = tx_id;
        state_->active_transactions.insert(std::move(tx_id));
    }

    void unregister_active_transaction(mt::TxId tx_id) noexcept override {
        std::lock_guard lock(state_->mutex);
        state_->active_transactions.erase(tx_id);
    }

    std::optional<mt::DocumentEnvelope> read_snapshot(
        mt::CollectionId collection,
        std::string_view key,
        mt::Version version) override {
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        auto it = c.history.find(std::string(key));
        if (it == c.history.end()) {
            return std::nullopt;
        }

        const MemoryVersion* best = nullptr;
        for (const auto& candidate : it->second) {
            if (candidate.version <= version) {
                if (!best || candidate.version > best->version) {
                    best = &candidate;
                }
            }
        }

        if (!best) {
            return std::nullopt;
        }

        return mt::DocumentEnvelope{
            .collection = collection,
            .key = best->key,
            .version = best->version,
            .deleted = best->deleted,
            .value_hash = best->hash,
            .value = best->value
        };
    }

    std::optional<mt::DocumentMetadata> read_current_metadata(
        mt::CollectionId collection,
        std::string_view key) override {
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        auto it = c.current.find(std::string(key));
        if (it == c.current.end()) {
            return std::nullopt;
        }

        return mt::DocumentMetadata{
            .collection = collection,
            .key = it->first,
            .version = it->second.version,
            .deleted = it->second.deleted,
            .value_hash = it->second.hash
        };
    }

    mt::QueryResultEnvelope query_snapshot(
        mt::CollectionId collection,
        const mt::QuerySpec& query,
        mt::Version version) override {
        // This fake backend supports only key-prefix query predicates.
        std::string prefix;
        for (const auto& predicate : query.predicates) {
            if (predicate.op == mt::QueryOp::KeyPrefix) {
                prefix = predicate.text;
            }
        }

        mt::ListOptions options;
        options.limit = query.limit;
        options.after_key = query.after_key;

        auto result = list_snapshot(collection, options, version);
        if (prefix.empty()) {
            return result;
        }

        mt::QueryResultEnvelope filtered;
        for (const auto& row : result.rows) {
            if (row.key.rfind(prefix, 0) == 0) {
                filtered.rows.push_back(row);
            }
        }
        return filtered;
    }

    mt::QueryMetadataResult query_current_metadata(
        mt::CollectionId collection,
        const mt::QuerySpec& query) override {
        std::string prefix;
        for (const auto& predicate : query.predicates) {
            if (predicate.op == mt::QueryOp::KeyPrefix) {
                prefix = predicate.text;
            }
        }

        mt::ListOptions options;
        options.limit = query.limit;
        options.after_key = query.after_key;

        auto result = list_current_metadata(collection, options);
        if (prefix.empty()) {
            return result;
        }

        mt::QueryMetadataResult filtered;
        for (const auto& row : result.rows) {
            if (row.key.rfind(prefix, 0) == 0) {
                filtered.rows.push_back(row);
            }
        }
        return filtered;
    }

    mt::QueryResultEnvelope list_snapshot(
        mt::CollectionId collection,
        const mt::ListOptions& options,
        mt::Version version) override {
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        mt::QueryResultEnvelope result;
        for (const auto& [key, versions] : c.history) {
            if (options.after_key && key <= *options.after_key) {
                continue;
            }

            const MemoryVersion* best = nullptr;
            for (const auto& candidate : versions) {
                if (candidate.version <= version) {
                    if (!best || candidate.version > best->version) {
                        best = &candidate;
                    }
                }
            }

            if (!best || best->deleted) {
                continue;
            }

            result.rows.push_back(mt::DocumentEnvelope{
                .collection = collection,
                .key = key,
                .version = best->version,
                .deleted = false,
                .value_hash = best->hash,
                .value = best->value
            });

            if (options.limit && result.rows.size() >= *options.limit) {
                break;
            }
        }

        return result;
    }

    mt::QueryMetadataResult list_current_metadata(
        mt::CollectionId collection,
        const mt::ListOptions& options) override {
        std::lock_guard lock(state_->mutex);
        const auto& c = collection_ref(collection);

        mt::QueryMetadataResult result;
        for (const auto& [key, current] : c.current) {
            if (options.after_key && key <= *options.after_key) {
                continue;
            }
            if (current.deleted) {
                continue;
            }

            result.rows.push_back(mt::DocumentMetadata{
                .collection = collection,
                .key = key,
                .version = current.version,
                .deleted = current.deleted,
                .value_hash = current.hash
            });

            if (options.limit && result.rows.size() >= *options.limit) {
                break;
            }
        }
        return result;
    }

    void insert_history(
        mt::CollectionId collection,
        const mt::WriteEnvelope& write,
        mt::Version commit_version) override {
        std::lock_guard lock(state_->mutex);
        auto& c = collection_mut(collection);

        MemoryVersion version{
            .version = commit_version,
            .deleted = write.kind == mt::WriteKind::Delete,
            .value = write.value,
            .hash = write.value_hash,
            .key = write.key
        };

        c.history[write.key].push_back(std::move(version));
    }

    void upsert_current(
        mt::CollectionId collection,
        const mt::WriteEnvelope& write,
        mt::Version commit_version) override {
        std::lock_guard lock(state_->mutex);
        auto& c = collection_mut(collection);

        c.current[write.key] = MemoryVersion{
            .version = commit_version,
            .deleted = write.kind == mt::WriteKind::Delete,
            .value = write.value,
            .hash = write.value_hash,
            .key = write.key
        };
    }

private:
    void require_backend_tx() const {
        if (!in_backend_tx_) {
            throw mt::BackendError("operation requires backend transaction");
        }
    }

    void release_clock_if_locked() noexcept {
        if (!owns_clock_lock_) {
            return;
        }

        std::lock_guard lock(state_->mutex);
        state_->clock_locked = false;
        owns_clock_lock_ = false;
    }

    const MemoryCollection& collection_ref(mt::CollectionId id) const {
        auto it = state_->collections.find(id);
        if (it == state_->collections.end()) {
            throw mt::BackendError("unknown collection");
        }
        return it->second;
    }

    MemoryCollection& collection_mut(mt::CollectionId id) {
        auto it = state_->collections.find(id);
        if (it == state_->collections.end()) {
            throw mt::BackendError("unknown collection");
        }
        return it->second;
    }

private:
    std::shared_ptr<MemoryState> state_;
    bool in_backend_tx_ = false;
    bool owns_clock_lock_ = false;
    mt::TxId active_tx_id_;
};

class MemoryBackend final : public mt::IDatabaseBackend {
public:
    MemoryBackend()
        : state_(std::make_shared<MemoryState>()) {}

    std::unique_ptr<mt::IBackendSession> open_session() override {
        return std::make_unique<MemorySession>(state_);
    }

    void bootstrap(const mt::BootstrapSpec&) override {}

    mt::CollectionDescriptor ensure_collection(const mt::CollectionSpec& spec) override {
        std::lock_guard lock(state_->mutex);

        auto existing = state_->descriptors_by_name.find(spec.logical_name);
        if (existing != state_->descriptors_by_name.end()) {
            return existing->second;
        }

        mt::CollectionDescriptor descriptor{
            .id = state_->next_collection_id++,
            .logical_name = spec.logical_name,
            .schema_version = spec.schema_version
        };

        MemoryCollection collection;
        collection.descriptor = descriptor;

        state_->descriptors_by_name[spec.logical_name] = descriptor;
        state_->collections[descriptor.id] = std::move(collection);

        return descriptor;
    }

    mt::CollectionDescriptor get_collection(std::string_view logical_name) override {
        std::lock_guard lock(state_->mutex);
        return state_->descriptors_by_name.at(std::string(logical_name));
    }

private:
    std::shared_ptr<MemoryState> state_;
};

struct Harness {
    std::shared_ptr<MemoryBackend> backend = std::make_shared<MemoryBackend>();
    mt::Database db{backend};
    mt::TransactionProvider txs{db};
    mt::TableProvider tables{db};
    mt::Table<User, UserMapping> users = tables.table<User, UserMapping>();
};

} // namespace test_support

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

#define EXPECT_TRUE(expr) assert((expr))
#define EXPECT_FALSE(expr) assert(!(expr))
#define EXPECT_EQ(a, b) assert((a) == (b))

#define EXPECT_THROW_AS(statement, exception_type)        \
    do {                                                  \
        bool did_throw = false;                           \
        try {                                             \
            statement;                                    \
        } catch (const exception_type&) {                 \
            did_throw = true;                             \
        }                                                 \
        assert(did_throw && "expected exception not thrown"); \
    } while (false)

using test_support::Harness;
using test_support::User;
using test_support::UserMapping;

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

void test_table_provider_creates_table() {
    Harness h;
    EXPECT_EQ(h.users.descriptor().logical_name, std::string("users"));
    EXPECT_TRUE(h.users.descriptor().id != 0);
}

void test_non_transactional_get_missing_returns_nullopt() {
    Harness h;
    auto user = h.users.get("user:missing");
    EXPECT_FALSE(user.has_value());
}

void test_transactional_put_then_non_transactional_get() {
    Harness h;

    h.txs.run([&](mt::Transaction& tx) {
        h.users.put(tx, User{
            .id = "user:1",
            .email = "a@example.com",
            .name = "Alice",
            .active = true,
            .login_count = 1
        });
    });

    auto loaded = h.users.get("user:1");
    EXPECT_TRUE(loaded.has_value());
    // With the placeholder Json implementation, full value round-trip requires
    // replacing mt::Json with a real JSON type. This assertion verifies presence.
}

void test_require_missing_throws() {
    Harness h;
    EXPECT_THROW_AS(h.users.require("missing"), mt::DocumentNotFound);
}

void test_transactional_read_your_own_point_write() {
    Harness h;

    h.txs.run([&](mt::Transaction& tx) {
        h.users.put(tx, User{
            .id = "user:1",
            .email = "a@example.com",
            .name = "Alice",
            .active = true,
            .login_count = 1
        });

        auto loaded = h.users.get(tx, "user:1");
        EXPECT_TRUE(loaded.has_value());
    });
}

void test_delete_hides_document() {
    Harness h;

    h.txs.run([&](mt::Transaction& tx) {
        h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    });

    h.txs.run([&](mt::Transaction& tx) {
        h.users.erase(tx, "user:1");
    });

    auto loaded = h.users.get("user:1");
    EXPECT_FALSE(loaded.has_value());
}

void test_write_write_conflict_aborts_later_committer() {
    Harness h;

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    h.users.put(tx1, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    h.users.put(tx2, User{.id = "user:1", .email = "b@example.com", .name = "Bob"});

    tx1.commit();
    EXPECT_THROW_AS(tx2.commit(), mt::TransactionConflict);
}

void test_read_write_conflict_aborts_reader() {
    Harness h;

    h.txs.run([&](mt::Transaction& tx) {
        h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    });

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    auto loaded = h.users.get(tx1, "user:1");
    EXPECT_TRUE(loaded.has_value());

    h.users.put(tx2, User{.id = "user:1", .email = "updated@example.com", .name = "Updated"});
    tx2.commit();

    h.users.put(tx1, User{.id = "user:2", .email = "new@example.com", .name = "New"});
    EXPECT_THROW_AS(tx1.commit(), mt::TransactionConflict);
}

void test_absent_read_conflicts_with_later_insert() {
    Harness h;

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    auto missing = h.users.get(tx1, "user:missing");
    EXPECT_FALSE(missing.has_value());

    h.users.put(tx2, User{.id = "user:missing", .email = "x@example.com", .name = "Inserted"});
    tx2.commit();

    h.users.put(tx1, User{.id = "user:other", .email = "o@example.com", .name = "Other"});
    EXPECT_THROW_AS(tx1.commit(), mt::TransactionConflict);
}

void test_list_predicate_conflict_on_insert() {
    Harness h;

    h.txs.run([&](mt::Transaction& tx) {
        h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    });

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    auto listed = h.users.list(tx1);
    EXPECT_TRUE(listed.size() >= 1);

    h.users.put(tx2, User{.id = "user:2", .email = "b@example.com", .name = "Bob"});
    tx2.commit();

    h.users.put(tx1, User{.id = "user:3", .email = "c@example.com", .name = "Carol"});
    EXPECT_THROW_AS(tx1.commit(), mt::TransactionConflict);
}

void test_query_predicate_conflict_on_matching_insert() {
    Harness h;

    auto tx1 = h.txs.begin();
    auto tx2 = h.txs.begin();

    auto query = mt::QuerySpec::key_prefix("user:");
    auto rows = h.users.query(tx1, query);
    EXPECT_TRUE(rows.empty());

    h.users.put(tx2, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    tx2.commit();

    h.users.put(tx1, User{.id = "other:1", .email = "o@example.com", .name = "Other"});
    EXPECT_THROW_AS(tx1.commit(), mt::TransactionConflict);
}

void test_retry_retries_on_conflict() {
    Harness h;

    h.txs.run([&](mt::Transaction& tx) {
        h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    });

    int attempts = 0;

    auto fn = [&](mt::Transaction& tx) {
        ++attempts;

        auto user = h.users.require(tx, "user:1");
        user.login_count += 1;

        if (attempts == 1) {
            h.txs.run([&](mt::Transaction& other) {
                h.users.put(other, User{
                    .id = "user:1",
                    .email = "external@example.com",
                    .name = "External",
                    .active = true,
                    .login_count = 99
                });
            });
        }

        h.users.put(tx, user);
    };

    mt::RetryPolicy policy;
    policy.max_attempts = 2;

    h.txs.retry(policy, fn);
    EXPECT_EQ(attempts, 2);
}

void test_rollback_discards_writes() {
    Harness h;

    {
        auto tx = h.txs.begin();
        h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
        tx.rollback();
    }

    auto loaded = h.users.get("user:1");
    EXPECT_FALSE(loaded.has_value());
}

void test_destructor_rolls_back_uncommitted_transaction() {
    Harness h;

    {
        auto tx = h.txs.begin();
        h.users.put(tx, User{.id = "user:1", .email = "a@example.com", .name = "Alice"});
    }

    auto loaded = h.users.get("user:1");
    EXPECT_FALSE(loaded.has_value());
}

int main() {
    test_table_provider_creates_table();
    test_non_transactional_get_missing_returns_nullopt();
    test_transactional_put_then_non_transactional_get();
    test_require_missing_throws();
    test_transactional_read_your_own_point_write();
    test_delete_hides_document();
    test_write_write_conflict_aborts_later_committer();
    test_read_write_conflict_aborts_reader();
    test_absent_read_conflicts_with_later_insert();
    test_list_predicate_conflict_on_insert();
    test_query_predicate_conflict_on_matching_insert();
    test_retry_retries_on_conflict();
    test_rollback_discards_writes();
    test_destructor_rolls_back_uncommitted_transaction();

    std::cout << "All mt_core tests passed.\n";
    return 0;
}
