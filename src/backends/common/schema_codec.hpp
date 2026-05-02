#pragma once

#include "mt/collection.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace mt::backends::common
{

std::string serialize_fields(const std::vector<FieldSpec>& fields);
std::vector<FieldSpec> deserialize_fields(std::string_view encoded);

std::string serialize_indexes(const std::vector<IndexSpec>& indexes);
std::vector<IndexSpec> deserialize_indexes(std::string_view encoded);

} // namespace mt::backends::common
