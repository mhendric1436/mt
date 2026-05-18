#pragma once

#include "mt/hash.hpp"
#include "mt/vendor/statespec_json.hpp"

#include <cstdint>
#include <vector>

// -----------------------------------------------------------------------------
// mt/json.hpp
//
// JSON value type and stable hashing. Json is an alias for
// statespec::backend::Json so that mt and statespec share one type with no
// conversion at the backend adapter boundary.
// -----------------------------------------------------------------------------

namespace mt
{

using Json = statespec::backend::Json;

inline Hash hash_json(const Json& value)
{
    constexpr std::uint64_t fnv_offset = 14695981039346656037ULL;
    constexpr std::uint64_t fnv_prime = 1099511628211ULL;

    auto canonical = value.canonical_string();
    auto hash = fnv_offset;

    for (unsigned char byte : canonical)
    {
        hash ^= byte;
        hash *= fnv_prime;
    }

    std::vector<std::uint8_t> bytes(sizeof(hash));
    for (std::size_t i = 0; i < bytes.size(); ++i)
    {
        bytes[i] = static_cast<std::uint8_t>((hash >> ((bytes.size() - 1 - i) * 8)) & 0xff);
    }

    return Hash{std::move(bytes)};
}

} // namespace mt
