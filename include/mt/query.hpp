#pragma once

#include "mt/errors.hpp"
#include "mt/json.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------
// mt/query.hpp
//
// Query and index model.
// -----------------------------------------------------------------------------

namespace mt
{

using Key = std::string;

struct ListOptions
{
    std::optional<std::size_t> limit;
    std::optional<Key> after_key;
};

enum class QueryOp
{
    JsonEquals,
    JsonContains,
    KeyPrefix
};

struct QueryPredicate
{
    QueryOp op{};
    std::string path;
    Json value;
    std::string text;
};

struct QuerySpec
{
    std::vector<QueryPredicate> predicates;
    std::optional<std::size_t> limit;
    std::optional<Key> after_key;
    bool order_by_key = true;

    static QuerySpec where_json_eq(
        std::string path,
        Json value
    )
    {
        QuerySpec q;
        q.predicates.push_back(
            QueryPredicate{
                .op = QueryOp::JsonEquals, .path = std::move(path), .value = std::move(value)
            }
        );
        return q;
    }

    static QuerySpec key_prefix(std::string prefix)
    {
        QuerySpec q;
        q.predicates.push_back(QueryPredicate{.op = QueryOp::KeyPrefix, .text = std::move(prefix)});
        return q;
    }
};

struct IndexSpec
{
    std::string name;
    std::string json_path;
    bool unique = false;

    static IndexSpec json_path_index(
        std::string name,
        std::string path
    )
    {
        return IndexSpec{.name = std::move(name), .json_path = std::move(path), .unique = false};
    }

    IndexSpec& make_unique()
    {
        unique = true;
        return *this;
    }
};

inline std::optional<Json> json_path_value(
    const Json& value,
    const std::string& path
)
{
    if (path == "$")
    {
        return value;
    }
    if (path.rfind("$.", 0) != 0)
    {
        throw BackendError("JSON paths must be '$' or start with '$.'");
    }

    const Json* current = &value;
    std::size_t start = 2;
    while (start <= path.size())
    {
        auto end = path.find('.', start);
        auto segment =
            path.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (segment.empty() || !current->is_object())
        {
            return std::nullopt;
        }

        const auto& object = current->as_object();
        auto it = object.find(segment);
        if (it == object.end())
        {
            return std::nullopt;
        }

        current = &it->second;
        if (end == std::string::npos)
        {
            break;
        }
        start = end + 1;
    }

    return *current;
}

inline bool matches_after_key(
    const Key& key,
    const std::optional<Key>& after_key
)
{
    return !after_key || key > *after_key;
}

inline bool matches_query(
    const Key& key,
    const Json& value,
    const QuerySpec& query
)
{
    if (!matches_after_key(key, query.after_key))
    {
        return false;
    }

    for (const auto& predicate : query.predicates)
    {
        switch (predicate.op)
        {
        case QueryOp::KeyPrefix:
            if (key.rfind(predicate.text, 0) != 0)
            {
                return false;
            }
            break;

        case QueryOp::JsonEquals:
            if (auto field = json_path_value(value, predicate.path))
            {
                if (*field != predicate.value)
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
            break;

        case QueryOp::JsonContains:
            throw BackendError("JSON contains predicates are not supported");
        }
    }

    return true;
}

} // namespace mt
