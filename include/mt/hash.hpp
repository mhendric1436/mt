#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
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

inline char hex_digit(std::uint8_t value)
{
    return static_cast<char>(value < 10 ? '0' + value : 'a' + (value - 10));
}

inline std::uint8_t hex_value(char value)
{
    if (value >= '0' && value <= '9')
    {
        return static_cast<std::uint8_t>(value - '0');
    }
    if (value >= 'a' && value <= 'f')
    {
        return static_cast<std::uint8_t>(10 + value - 'a');
    }
    if (value >= 'A' && value <= 'F')
    {
        return static_cast<std::uint8_t>(10 + value - 'A');
    }
    throw std::invalid_argument("invalid hex digit");
}

inline std::string hash_to_text(const Hash& hash)
{
    std::string out;
    out.reserve(hash.bytes.size() * 2);
    for (auto byte : hash.bytes)
    {
        out.push_back(hex_digit(static_cast<std::uint8_t>(byte >> 4)));
        out.push_back(hex_digit(static_cast<std::uint8_t>(byte & 0x0F)));
    }
    return out;
}

} // namespace mt
