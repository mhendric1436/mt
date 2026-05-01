#include "sqlite_document.hpp"

#include "mt/errors.hpp"
#include "mt/json_parser.hpp"

#include <cstdint>

namespace mt::backends::sqlite
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

Json parse_stored_json(const std::string& encoded)
{
    return parse_json(encoded);
}

} // namespace mt::backends::sqlite
