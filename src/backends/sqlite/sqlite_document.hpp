#pragma once

#include "mt/hash.hpp"
#include "mt/json.hpp"
#include "mt/types.hpp"

#include <string>

namespace mt::backends::sqlite
{

Hash hash_from_text(const std::string& encoded);

bool write_is_deleted(const WriteEnvelope& write);

std::string write_value_text(const WriteEnvelope& write);

Json parse_stored_json(const std::string& encoded);

} // namespace mt::backends::sqlite
