#pragma once

#include <cstdint>
#include <vector>

// -----------------------------------------------------------------------------
// mt_json.hpp
//
// Basic JSON placeholder and hashing.
// -----------------------------------------------------------------------------

namespace mt
{

class Json
{
  public:
    Json() = default;

    // Placeholder constructors for examples. Replace in production.
    Json(std::nullptr_t) {}

    friend bool operator==(
        const Json&,
        const Json&
    ) = default;
};

struct Hash
{
    std::vector<std::uint8_t> bytes;

    friend bool operator==(
        const Hash&,
        const Hash&
    ) = default;
};

// Default hash placeholder. A production implementation should use a stable
// cryptographic or strong non-cryptographic hash over canonical JSON bytes.
inline Hash hash_json(const Json&)
{
    return Hash{{0}};
}

} // namespace mt
