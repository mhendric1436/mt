#pragma once

#include <cstdint>
#include <vector>

// -----------------------------------------------------------------------------
// mt/hash.hpp
//
// Hash value type used by document metadata and JSON hashing.
// -----------------------------------------------------------------------------

namespace mt
{

struct Hash
{
    std::vector<std::uint8_t> bytes;

    friend bool operator==(
        const Hash&,
        const Hash&
    ) = default;
};

} // namespace mt
