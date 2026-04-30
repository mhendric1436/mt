#include "mt/backends/sqlite.hpp"

#include "sqlite_detail.hpp"

#include "mt/errors.hpp"
#include "mt/schema.hpp"

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------
// SQLite backend implementation unit.
//
// The public header remains dependency-free. SQLite client details stay in this
// optional implementation unit and private helpers.
// -----------------------------------------------------------------------------

namespace mt::backends::sqlite
{

namespace
{
Hash hash_from_text(const std::string& encoded)
{
    if (encoded.size() % 2 != 0)
    {
        throw BackendError("invalid stored SQLite hash");
    }

    Hash hash;
    hash.bytes.reserve(encoded.size() / 2);
    for (std::size_t i = 0; i < encoded.size(); i += 2)
    {
        hash.bytes.push_back(
            static_cast<std::uint8_t>((hex_value(encoded[i]) << 4) | hex_value(encoded[i + 1]))
        );
    }
    return hash;
}

bool write_is_deleted(const WriteEnvelope& write)
{
    return write.kind == WriteKind::Delete;
}

std::string write_value_text(const WriteEnvelope& write)
{
    if (write_is_deleted(write))
    {
        return {};
    }
    return write.value.canonical_string();
}

class JsonParser
{
  public:
    explicit JsonParser(std::string_view input)
        : input_(input)
    {
    }

    Json parse()
    {
        auto value = parse_value();
        if (position_ != input_.size())
        {
            fail();
        }
        return value;
    }

  private:
    [[noreturn]] void fail() const
    {
        throw BackendError("invalid stored SQLite JSON value");
    }

    bool consume(char expected)
    {
        if (position_ < input_.size() && input_[position_] == expected)
        {
            ++position_;
            return true;
        }
        return false;
    }

    void expect(char expected)
    {
        if (!consume(expected))
        {
            fail();
        }
    }

    bool starts_with(std::string_view value) const
    {
        return input_.substr(position_, value.size()) == value;
    }

    Json parse_value()
    {
        if (starts_with("null"))
        {
            position_ += 4;
            return Json::null();
        }
        if (starts_with("true"))
        {
            position_ += 4;
            return Json(true);
        }
        if (starts_with("false"))
        {
            position_ += 5;
            return Json(false);
        }
        if (position_ >= input_.size())
        {
            fail();
        }

        switch (input_[position_])
        {
        case '"':
            return Json(parse_string());
        case '[':
            return parse_array();
        case '{':
            return parse_object();
        default:
            return parse_number();
        }
    }

    std::string parse_string()
    {
        expect('"');
        std::string out;
        while (position_ < input_.size())
        {
            auto c = input_[position_++];
            if (c == '"')
            {
                return out;
            }
            if (c != '\\')
            {
                out.push_back(c);
                continue;
            }
            if (position_ >= input_.size())
            {
                fail();
            }

            auto escaped = input_[position_++];
            switch (escaped)
            {
            case '"':
            case '\\':
            case '/':
                out.push_back(escaped);
                break;
            case 'b':
                out.push_back('\b');
                break;
            case 'f':
                out.push_back('\f');
                break;
            case 'n':
                out.push_back('\n');
                break;
            case 'r':
                out.push_back('\r');
                break;
            case 't':
                out.push_back('\t');
                break;
            case 'u':
                out.push_back(parse_basic_unicode_escape());
                break;
            default:
                fail();
            }
        }
        fail();
    }

    char parse_basic_unicode_escape()
    {
        if (position_ + 4 > input_.size())
        {
            fail();
        }

        auto value = 0U;
        for (auto i = 0; i < 4; ++i)
        {
            value = (value << 4) | hex_value(input_[position_++]);
        }
        if (value > 0x7F)
        {
            fail();
        }
        return static_cast<char>(value);
    }

    Json parse_array()
    {
        expect('[');
        Json::Array values;
        if (consume(']'))
        {
            return Json::array(std::move(values));
        }

        while (true)
        {
            values.push_back(parse_value());
            if (consume(']'))
            {
                return Json::array(std::move(values));
            }
            expect(',');
        }
    }

    Json parse_object()
    {
        expect('{');
        Json::Object object;
        if (consume('}'))
        {
            return Json::object(std::move(object));
        }

        while (true)
        {
            auto key = parse_string();
            expect(':');
            object.insert_or_assign(std::move(key), parse_value());
            if (consume('}'))
            {
                return Json::object(std::move(object));
            }
            expect(',');
        }
    }

    Json parse_number()
    {
        auto start = position_;
        consume('-');
        if (position_ >= input_.size() ||
            !std::isdigit(static_cast<unsigned char>(input_[position_])))
        {
            fail();
        }

        while (position_ < input_.size() &&
               std::isdigit(static_cast<unsigned char>(input_[position_])))
        {
            ++position_;
        }

        auto is_double = false;
        if (consume('.'))
        {
            is_double = true;
            if (position_ >= input_.size() ||
                !std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                fail();
            }
            while (position_ < input_.size() &&
                   std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                ++position_;
            }
        }

        if (position_ < input_.size() && (input_[position_] == 'e' || input_[position_] == 'E'))
        {
            is_double = true;
            ++position_;
            if (position_ < input_.size() && (input_[position_] == '+' || input_[position_] == '-'))
            {
                ++position_;
            }
            if (position_ >= input_.size() ||
                !std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                fail();
            }
            while (position_ < input_.size() &&
                   std::isdigit(static_cast<unsigned char>(input_[position_])))
            {
                ++position_;
            }
        }

        auto text = std::string(input_.substr(start, position_ - start));
        try
        {
            if (is_double)
            {
                return Json(std::stod(text));
            }
            return Json(static_cast<std::int64_t>(std::stoll(text)));
        }
        catch (const std::exception&)
        {
            fail();
        }
    }

  private:
    std::string_view input_;
    std::size_t position_ = 0;
};

Json parse_stored_json(const std::string& encoded)
{
    return JsonParser(encoded).parse();
}

void validate_supported_query(const QuerySpec& query)
{
    if (!query.order_by_key)
    {
        throw BackendError("sqlite backend only supports key ordering");
    }

    for (const auto& predicate : query.predicates)
    {
        if (predicate.op == QueryOp::JsonContains)
        {
            throw BackendError("sqlite backend does not support JSON contains predicates");
        }
    }
}

int field_type_to_int(FieldType type)
{
    return static_cast<int>(type);
}

FieldType field_type_from_int(int value)
{
    switch (static_cast<FieldType>(value))
    {
    case FieldType::String:
    case FieldType::Bool:
    case FieldType::Int64:
    case FieldType::Double:
    case FieldType::Optional:
    case FieldType::Array:
    case FieldType::Object:
        return static_cast<FieldType>(value);
    }

    throw BackendError("invalid stored SQLite field type");
}

std::string default_payload(const Json& value)
{
    if (value.is_null())
    {
        return "null";
    }
    if (value.is_bool())
    {
        return value.as_bool() ? "true" : "false";
    }
    if (value.is_int64())
    {
        return std::to_string(value.as_int64());
    }
    if (value.is_double())
    {
        std::ostringstream stream;
        stream << std::setprecision(17) << value.as_double();
        return stream.str();
    }
    if (value.is_string())
    {
        return value.as_string();
    }

    throw BackendError("SQLite schema snapshots support scalar default values only");
}

char default_kind(const Json& value)
{
    if (value.is_null())
    {
        return 'z';
    }
    if (value.is_bool())
    {
        return 'b';
    }
    if (value.is_int64())
    {
        return 'i';
    }
    if (value.is_double())
    {
        return 'd';
    }
    if (value.is_string())
    {
        return 's';
    }

    throw BackendError("SQLite schema snapshots support scalar default values only");
}

Json default_json(
    char kind,
    const std::string& payload
)
{
    switch (kind)
    {
    case 'z':
        return Json::null();
    case 'b':
        return Json(payload == "true");
    case 'i':
        return Json(static_cast<std::int64_t>(std::stoll(payload)));
    case 'd':
        return Json(std::stod(payload));
    case 's':
        return Json(payload);
    }

    throw BackendError("invalid stored SQLite default value kind");
}

void append_field(
    std::ostream& out,
    const FieldSpec& field
)
{
    auto kind = field.has_default ? default_kind(field.default_value) : '-';
    auto payload = field.has_default ? default_payload(field.default_value) : std::string{};

    out << "field " << std::quoted(field.name) << ' ' << field_type_to_int(field.type) << ' '
        << (field.required ? 1 : 0) << ' ' << (field.has_default ? 1 : 0) << ' '
        << field_type_to_int(field.value_type) << ' ' << kind << ' ' << std::quoted(payload) << ' '
        << field.fields.size() << '\n';

    for (const auto& child : field.fields)
    {
        append_field(out, child);
    }
}

FieldSpec read_field(std::istream& in)
{
    std::string marker;
    std::string name;
    int type = 0;
    int required = 0;
    int has_default = 0;
    int value_type = 0;
    char kind = '-';
    std::string payload;
    std::size_t child_count = 0;

    if (!(in >> marker >> std::quoted(name) >> type >> required >> has_default >> value_type >>
          kind >> std::quoted(payload) >> child_count) ||
        marker != "field")
    {
        throw BackendError("invalid stored SQLite field metadata");
    }

    FieldSpec field{
        .name = std::move(name),
        .type = field_type_from_int(type),
        .required = required != 0,
        .has_default = has_default != 0,
        .value_type = field_type_from_int(value_type)
    };
    if (field.has_default)
    {
        field.default_value = default_json(kind, payload);
    }

    field.fields.reserve(child_count);
    for (std::size_t i = 0; i < child_count; ++i)
    {
        field.fields.push_back(read_field(in));
    }

    return field;
}

std::string serialize_fields(const std::vector<FieldSpec>& fields)
{
    std::ostringstream out;
    out << "fields " << fields.size() << '\n';
    for (const auto& field : fields)
    {
        append_field(out, field);
    }
    return out.str();
}

std::vector<FieldSpec> deserialize_fields(const std::string& encoded)
{
    std::istringstream in(encoded);
    std::string marker;
    std::size_t count = 0;
    if (!(in >> marker >> count) || marker != "fields")
    {
        throw BackendError("invalid stored SQLite schema metadata");
    }

    std::vector<FieldSpec> fields;
    fields.reserve(count);
    for (std::size_t i = 0; i < count; ++i)
    {
        fields.push_back(read_field(in));
    }
    return fields;
}

std::string serialize_indexes(const std::vector<IndexSpec>& indexes)
{
    std::ostringstream out;
    out << "indexes " << indexes.size() << '\n';
    for (const auto& index : indexes)
    {
        out << "index " << std::quoted(index.name) << ' ' << std::quoted(index.json_path) << ' '
            << (index.unique ? 1 : 0) << '\n';
    }
    return out.str();
}

std::vector<IndexSpec> deserialize_indexes(const std::string& encoded)
{
    std::istringstream in(encoded);
    std::string marker;
    std::size_t count = 0;
    if (!(in >> marker >> count) || marker != "indexes")
    {
        throw BackendError("invalid stored SQLite index metadata");
    }

    std::vector<IndexSpec> indexes;
    indexes.reserve(count);
    for (std::size_t i = 0; i < count; ++i)
    {
        std::string item_marker;
        IndexSpec index;
        int unique = 0;
        if (!(in >> item_marker >> std::quoted(index.name) >> std::quoted(index.json_path) >>
              unique) ||
            item_marker != "index")
        {
            throw BackendError("invalid stored SQLite index metadata");
        }
        index.unique = unique != 0;
        indexes.push_back(std::move(index));
    }

    return indexes;
}

std::optional<CollectionSpec> load_collection_spec(
    detail::Connection& connection,
    std::string_view logical_name
)
{
    detail::Statement statement{
        connection.get(),
        "SELECT logical_name, schema_version, key_field, schema_json, indexes_json "
        "FROM mt_collections WHERE logical_name = ?"
    };
    statement.bind_text(1, logical_name);
    if (!statement.step())
    {
        return std::nullopt;
    }

    auto schema_json = statement.column_text(3);
    auto indexes_json = statement.column_text(4);
    return CollectionSpec{
        .logical_name = statement.column_text(0),
        .indexes = deserialize_indexes(indexes_json),
        .schema_version = static_cast<int>(statement.column_int64(1)),
        .key_field = statement.column_text(2),
        .fields = deserialize_fields(schema_json)
    };
}

CollectionDescriptor load_collection_descriptor(
    detail::Connection& connection,
    std::string_view logical_name
)
{
    detail::Statement statement{
        connection.get(),
        "SELECT id, logical_name, schema_version FROM mt_collections WHERE logical_name = ?"
    };
    statement.bind_text(1, logical_name);
    if (!statement.step())
    {
        throw BackendError("sqlite collection not found");
    }

    return CollectionDescriptor{
        .id = static_cast<CollectionId>(statement.column_int64(0)),
        .logical_name = statement.column_text(1),
        .schema_version = static_cast<int>(statement.column_int64(2))
    };
}

void insert_collection(
    detail::Connection& connection,
    const CollectionSpec& spec
)
{
    detail::Statement statement{
        connection.get(), "INSERT INTO mt_collections "
                          "(logical_name, schema_version, key_field, schema_json, indexes_json) "
                          "VALUES (?, ?, ?, ?, ?)"
    };
    statement.bind_text(1, spec.logical_name);
    statement.bind_int64(2, spec.schema_version);
    statement.bind_text(3, spec.key_field);
    statement.bind_text(4, serialize_fields(spec.fields));
    statement.bind_text(5, serialize_indexes(spec.indexes));
    statement.step();
}

void update_collection(
    detail::Connection& connection,
    const CollectionSpec& spec
)
{
    detail::Statement statement{
        connection.get(),
        "UPDATE mt_collections "
        "SET schema_version = ?, key_field = ?, schema_json = ?, indexes_json = ? "
        "WHERE logical_name = ?"
    };
    statement.bind_int64(1, spec.schema_version);
    statement.bind_text(2, spec.key_field);
    statement.bind_text(3, serialize_fields(spec.fields));
    statement.bind_text(4, serialize_indexes(spec.indexes));
    statement.bind_text(5, spec.logical_name);
    statement.step();
}

std::vector<IndexSpec> load_collection_indexes(
    detail::Connection& connection,
    CollectionId collection
)
{
    detail::Statement statement{
        connection.get(), "SELECT indexes_json FROM mt_collections WHERE id = ?"
    };
    statement.bind_int64(1, collection);
    if (!statement.step())
    {
        throw BackendError("sqlite collection not found");
    }
    return deserialize_indexes(statement.column_text(0));
}

void check_unique_constraints(
    detail::Connection& connection,
    CollectionId collection,
    const WriteEnvelope& write
)
{
    if (write.kind == WriteKind::Delete)
    {
        return;
    }

    for (const auto& index : load_collection_indexes(connection, collection))
    {
        if (!index.unique)
        {
            continue;
        }

        auto write_value = json_path_value(write.value, index.json_path);
        if (!write_value)
        {
            continue;
        }

        detail::Statement statement{
            connection.get(), "SELECT document_key, value_json "
                              "FROM mt_current "
                              "WHERE collection_id = ? AND deleted = 0 AND document_key <> ?"
        };
        statement.bind_int64(1, collection);
        statement.bind_text(2, write.key);

        while (statement.step())
        {
            if (statement.column_is_null(1))
            {
                continue;
            }

            auto current_value =
                json_path_value(parse_stored_json(statement.column_text(1)), index.json_path);
            if (current_value && *current_value == *write_value)
            {
                throw BackendError("sqlite backend unique index constraint violation");
            }
        }
    }
}

void bootstrap_schema(
    detail::Connection& connection,
    const BootstrapSpec& spec
)
{
    connection.execute("PRAGMA foreign_keys = ON");

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_meta ("
        "key TEXT PRIMARY KEY,"
        "value INTEGER NOT NULL"
        ")"
    );

    {
        detail::Statement statement{
            connection.get(),
            "INSERT INTO mt_meta (key, value) VALUES ('metadata_schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = MAX(value, excluded.value)"
        };
        statement.bind_int64(1, spec.metadata_schema_version);
        statement.step();
    }

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_clock ("
        "id INTEGER PRIMARY KEY CHECK (id = 1),"
        "version INTEGER NOT NULL,"
        "next_tx_id INTEGER NOT NULL"
        ")"
    );
    connection.execute("INSERT OR IGNORE INTO mt_clock (id, version, next_tx_id) VALUES (1, 0, 1)");

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_collections ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "logical_name TEXT NOT NULL UNIQUE,"
        "schema_version INTEGER NOT NULL,"
        "key_field TEXT NOT NULL,"
        "schema_json TEXT NOT NULL,"
        "indexes_json TEXT NOT NULL"
        ")"
    );

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_active_transactions ("
        "tx_id TEXT PRIMARY KEY,"
        "start_version INTEGER NOT NULL"
        ")"
    );

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_history ("
        "collection_id INTEGER NOT NULL,"
        "document_key TEXT NOT NULL,"
        "version INTEGER NOT NULL,"
        "deleted INTEGER NOT NULL,"
        "value_hash TEXT NOT NULL,"
        "value_json TEXT,"
        "PRIMARY KEY (collection_id, document_key, version),"
        "FOREIGN KEY (collection_id) REFERENCES mt_collections(id)"
        ")"
    );
    connection.execute(
        "CREATE INDEX IF NOT EXISTS mt_history_snapshot_idx "
        "ON mt_history (collection_id, document_key, version)"
    );

    connection.execute(
        "CREATE TABLE IF NOT EXISTS mt_current ("
        "collection_id INTEGER NOT NULL,"
        "document_key TEXT NOT NULL,"
        "version INTEGER NOT NULL,"
        "deleted INTEGER NOT NULL,"
        "value_hash TEXT NOT NULL,"
        "value_json TEXT,"
        "PRIMARY KEY (collection_id, document_key),"
        "FOREIGN KEY (collection_id) REFERENCES mt_collections(id)"
        ")"
    );
}

class SqliteSession final : public IBackendSession
{
  public:
    explicit SqliteSession(std::string path)
        : path_(std::move(path))
    {
    }

    void begin_backend_transaction() override
    {
        if (in_backend_tx_)
        {
            throw BackendError("sqlite backend transaction is already open");
        }

        connection_ = detail::Connection::open(path_);
        bootstrap_schema(*connection_, BootstrapSpec{});
        connection_->execute("BEGIN IMMEDIATE");
        in_backend_tx_ = true;
    }

    void commit_backend_transaction() override
    {
        require_backend_tx();
        connection_->execute("COMMIT");
        in_backend_tx_ = false;
        clock_locked_ = false;
        connection_.reset();
    }

    void abort_backend_transaction() noexcept override
    {
        if (connection_ && in_backend_tx_)
        {
            sqlite3_exec(connection_->get(), "ROLLBACK", nullptr, nullptr, nullptr);
        }
        in_backend_tx_ = false;
        clock_locked_ = false;
        connection_.reset();
    }

    Version read_clock() override
    {
        require_backend_tx();
        return read_clock_row();
    }

    Version lock_clock_and_read() override
    {
        require_backend_tx();
        if (clock_locked_)
        {
            throw BackendError("sqlite clock is already locked by this session");
        }

        auto version = read_clock_row();
        clock_locked_ = true;
        return version;
    }

    Version increment_clock_and_return() override
    {
        require_backend_tx();
        if (!clock_locked_)
        {
            throw BackendError("clock must be locked before increment");
        }

        connection_->execute("UPDATE mt_clock SET version = version + 1 WHERE id = 1");
        return read_clock_row();
    }

    TxId create_transaction_id() override
    {
        require_backend_tx();

        auto tx_number = std::int64_t{0};
        {
            detail::Statement statement{
                connection_->get(), "SELECT next_tx_id FROM mt_clock WHERE id = 1"
            };
            if (!statement.step())
            {
                throw BackendError("sqlite clock row is missing");
            }
            tx_number = statement.column_int64(0);
        }

        connection_->execute("UPDATE mt_clock SET next_tx_id = next_tx_id + 1 WHERE id = 1");
        return "sqlite:" + std::to_string(tx_number);
    }

    void register_active_transaction(
        TxId tx_id,
        Version start_version
    ) override
    {
        require_backend_tx();

        detail::Statement statement{
            connection_->get(),
            "INSERT OR REPLACE INTO mt_active_transactions (tx_id, start_version) VALUES (?, ?)"
        };
        statement.bind_text(1, tx_id);
        statement.bind_int64(2, start_version);
        statement.step();
    }

    void unregister_active_transaction(TxId tx_id) noexcept override
    {
        try
        {
            if (!connection_ || !in_backend_tx_)
            {
                return;
            }

            detail::Statement statement{
                connection_->get(), "DELETE FROM mt_active_transactions WHERE tx_id = ?"
            };
            statement.bind_text(1, tx_id);
            statement.step();
        }
        catch (...)
        {
        }
    }

    std::optional<DocumentEnvelope> read_snapshot(
        CollectionId collection,
        std::string_view key,
        Version version
    ) override
    {
        require_backend_tx();

        detail::Statement statement{
            connection_->get(), "SELECT version, deleted, value_hash, value_json "
                                "FROM mt_history "
                                "WHERE collection_id = ? AND document_key = ? AND version <= ? "
                                "ORDER BY version DESC LIMIT 1"
        };
        statement.bind_int64(1, collection);
        statement.bind_text(2, key);
        statement.bind_int64(3, version);
        if (!statement.step())
        {
            return std::nullopt;
        }

        auto deleted = statement.column_int64(1) != 0;
        return DocumentEnvelope{
            .collection = collection,
            .key = std::string(key),
            .version = static_cast<Version>(statement.column_int64(0)),
            .deleted = deleted,
            .value_hash = hash_from_text(statement.column_text(2)),
            .value = deleted ? Json::null() : parse_stored_json(statement.column_text(3))
        };
    }

    std::optional<DocumentMetadata> read_current_metadata(
        CollectionId collection,
        std::string_view key
    ) override
    {
        require_backend_tx();

        detail::Statement statement{
            connection_->get(), "SELECT version, deleted, value_hash "
                                "FROM mt_current "
                                "WHERE collection_id = ? AND document_key = ?"
        };
        statement.bind_int64(1, collection);
        statement.bind_text(2, key);
        if (!statement.step())
        {
            return std::nullopt;
        }

        return DocumentMetadata{
            .collection = collection,
            .key = std::string(key),
            .version = static_cast<Version>(statement.column_int64(0)),
            .deleted = statement.column_int64(1) != 0,
            .value_hash = hash_from_text(statement.column_text(2))
        };
    }

    QueryResultEnvelope query_snapshot(
        CollectionId collection,
        const QuerySpec& query,
        Version version
    ) override
    {
        require_backend_tx();
        validate_supported_query(query);

        auto candidates =
            list_snapshot(collection, ListOptions{.after_key = query.after_key}, version);

        QueryResultEnvelope result;
        for (const auto& row : candidates.rows)
        {
            if (row.deleted)
            {
                continue;
            }
            if (!matches_query(row.key, row.value, query))
            {
                continue;
            }

            result.rows.push_back(row);
            if (query.limit && result.rows.size() >= *query.limit)
            {
                break;
            }
        }

        return result;
    }

    QueryMetadataResult query_current_metadata(
        CollectionId collection,
        const QuerySpec& query
    ) override
    {
        require_backend_tx();
        validate_supported_query(query);

        auto sql = std::string(
            "SELECT document_key, version, deleted, value_hash, value_json "
            "FROM mt_current "
            "WHERE collection_id = ? "
        );
        if (query.after_key)
        {
            sql += "AND document_key > ? ";
        }
        sql += "ORDER BY document_key";

        detail::Statement statement{connection_->get(), sql};
        auto index = 1;
        statement.bind_int64(index++, collection);
        if (query.after_key)
        {
            statement.bind_text(index++, *query.after_key);
        }

        QueryMetadataResult result;
        while (statement.step())
        {
            if (statement.column_int64(2) != 0)
            {
                continue;
            }

            auto key = statement.column_text(0);
            auto value = parse_stored_json(statement.column_text(4));
            if (!matches_query(key, value, query))
            {
                continue;
            }

            result.rows.push_back(
                DocumentMetadata{
                    .collection = collection,
                    .key = std::move(key),
                    .version = static_cast<Version>(statement.column_int64(1)),
                    .deleted = false,
                    .value_hash = hash_from_text(statement.column_text(3))
                }
            );
            if (query.limit && result.rows.size() >= *query.limit)
            {
                break;
            }
        }

        return result;
    }

    QueryResultEnvelope list_snapshot(
        CollectionId collection,
        const ListOptions& options,
        Version version
    ) override
    {
        require_backend_tx();

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
        );
        if (options.after_key)
        {
            sql += "AND h.document_key > ? ";
        }
        sql += "ORDER BY h.document_key ";
        if (options.limit)
        {
            sql += "LIMIT ?";
        }

        detail::Statement statement{connection_->get(), sql};
        auto index = 1;
        statement.bind_int64(index++, collection);
        statement.bind_int64(index++, version);
        if (options.after_key)
        {
            statement.bind_text(index++, *options.after_key);
        }
        if (options.limit)
        {
            statement.bind_int64(index++, static_cast<std::int64_t>(*options.limit));
        }

        QueryResultEnvelope result;
        while (statement.step())
        {
            auto deleted = statement.column_int64(2) != 0;
            result.rows.push_back(
                DocumentEnvelope{
                    .collection = collection,
                    .key = statement.column_text(0),
                    .version = static_cast<Version>(statement.column_int64(1)),
                    .deleted = deleted,
                    .value_hash = hash_from_text(statement.column_text(3)),
                    .value = deleted ? Json::null() : parse_stored_json(statement.column_text(4))
                }
            );
        }

        return result;
    }

    QueryMetadataResult list_current_metadata(
        CollectionId collection,
        const ListOptions& options
    ) override
    {
        require_backend_tx();

        auto sql = std::string(
            "SELECT document_key, version, deleted, value_hash "
            "FROM mt_current "
            "WHERE collection_id = ? "
        );
        if (options.after_key)
        {
            sql += "AND document_key > ? ";
        }
        sql += "ORDER BY document_key ";
        if (options.limit)
        {
            sql += "LIMIT ?";
        }

        detail::Statement statement{connection_->get(), sql};
        auto index = 1;
        statement.bind_int64(index++, collection);
        if (options.after_key)
        {
            statement.bind_text(index++, *options.after_key);
        }
        if (options.limit)
        {
            statement.bind_int64(index++, static_cast<std::int64_t>(*options.limit));
        }

        QueryMetadataResult result;
        while (statement.step())
        {
            result.rows.push_back(
                DocumentMetadata{
                    .collection = collection,
                    .key = statement.column_text(0),
                    .version = static_cast<Version>(statement.column_int64(1)),
                    .deleted = statement.column_int64(2) != 0,
                    .value_hash = hash_from_text(statement.column_text(3))
                }
            );
        }

        return result;
    }

    void insert_history(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) override
    {
        require_backend_tx();

        detail::Statement statement{
            connection_->get(),
            "INSERT INTO mt_history "
            "(collection_id, document_key, version, deleted, value_hash, value_json) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        };
        statement.bind_int64(1, collection);
        statement.bind_text(2, write.key);
        statement.bind_int64(3, commit_version);
        statement.bind_int64(4, write_is_deleted(write) ? 1 : 0);
        statement.bind_text(5, hash_to_text(write.value_hash));
        if (write_is_deleted(write))
        {
            statement.bind_null(6);
        }
        else
        {
            statement.bind_text(6, write_value_text(write));
        }
        statement.step();
    }

    void upsert_current(
        CollectionId collection,
        const WriteEnvelope& write,
        Version commit_version
    ) override
    {
        require_backend_tx();
        check_unique_constraints(*connection_, collection, write);

        detail::Statement statement{
            connection_->get(),
            "INSERT INTO mt_current "
            "(collection_id, document_key, version, deleted, value_hash, value_json) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(collection_id, document_key) DO UPDATE SET "
            "version = excluded.version, "
            "deleted = excluded.deleted, "
            "value_hash = excluded.value_hash, "
            "value_json = excluded.value_json"
        };
        statement.bind_int64(1, collection);
        statement.bind_text(2, write.key);
        statement.bind_int64(3, commit_version);
        statement.bind_int64(4, write_is_deleted(write) ? 1 : 0);
        statement.bind_text(5, hash_to_text(write.value_hash));
        if (write_is_deleted(write))
        {
            statement.bind_null(6);
        }
        else
        {
            statement.bind_text(6, write_value_text(write));
        }
        statement.step();
    }

  private:
    void require_backend_tx() const
    {
        if (!connection_ || !in_backend_tx_)
        {
            throw BackendError("sqlite backend transaction is not open");
        }
    }

    Version read_clock_row()
    {
        detail::Statement statement{
            connection_->get(), "SELECT version FROM mt_clock WHERE id = 1"
        };
        if (!statement.step())
        {
            throw BackendError("sqlite clock row is missing");
        }
        return static_cast<Version>(statement.column_int64(0));
    }

  private:
    std::string path_;
    std::optional<detail::Connection> connection_;
    bool in_backend_tx_ = false;
    bool clock_locked_ = false;
};

} // namespace

SqliteBackend::SqliteBackend()
    : SqliteBackend(":memory:")
{
}

SqliteBackend::SqliteBackend(std::string path)
    : path_(std::move(path))
{
}

BackendCapabilities SqliteBackend::capabilities() const
{
    auto capabilities = BackendCapabilities{};
    capabilities.query.key_prefix = true;
    capabilities.query.json_equals = true;
    capabilities.schema.json_indexes = true;
    capabilities.schema.unique_indexes = true;
    return capabilities;
}

std::unique_ptr<IBackendSession> SqliteBackend::open_session()
{
    return std::make_unique<SqliteSession>(path_);
}

void SqliteBackend::bootstrap(const BootstrapSpec& spec)
{
    auto connection = detail::Connection::open(path_);
    bootstrap_schema(connection, spec);
}

CollectionDescriptor SqliteBackend::ensure_collection(const CollectionSpec& spec)
{
    auto connection = detail::Connection::open(path_);
    bootstrap_schema(connection, BootstrapSpec{});

    connection.execute("BEGIN IMMEDIATE");
    try
    {
        auto stored = load_collection_spec(connection, spec.logical_name);
        if (!stored)
        {
            insert_collection(connection, spec);
            auto descriptor = load_collection_descriptor(connection, spec.logical_name);
            connection.execute("COMMIT");
            return descriptor;
        }

        auto diff = diff_schemas(*stored, spec);
        if (!diff.is_compatible())
        {
            const auto& change = diff.incompatible_changes.front();
            throw BackendError(
                "incompatible schema change for collection '" + spec.logical_name + "' at " +
                change.path + ": " + change.message
            );
        }

        update_collection(connection, spec);
        auto descriptor = load_collection_descriptor(connection, spec.logical_name);
        connection.execute("COMMIT");
        return descriptor;
    }
    catch (...)
    {
        sqlite3_exec(connection.get(), "ROLLBACK", nullptr, nullptr, nullptr);
        throw;
    }
}

CollectionDescriptor SqliteBackend::get_collection(std::string_view logical_name)
{
    auto connection = detail::Connection::open(path_);
    bootstrap_schema(connection, BootstrapSpec{});
    return load_collection_descriptor(connection, logical_name);
}

} // namespace mt::backends::sqlite
