#pragma once

#include "mt_json.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------
// mt_query.hpp
//
// Query and index model.
// -----------------------------------------------------------------------------

namespace mt {

using Key = std::string;

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

} // namespace mt
