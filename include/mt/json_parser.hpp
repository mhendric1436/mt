#pragma once

#include "mt/errors.hpp"
#include "mt/json.hpp"

#include <string_view>

// -----------------------------------------------------------------------------
// mt/json_parser.hpp
//
// JSON parsing for mt. Delegates to statespec::backend::JsonParser and
// translates JsonError to BackendError so callers see a consistent mt error
// type.
// -----------------------------------------------------------------------------

namespace mt
{

inline Json parse_json(std::string_view encoded)
{
    try
    {
        return statespec::backend::Json::parse(encoded);
    }
    catch (const statespec::backend::JsonError& e)
    {
        throw BackendError(e.what());
    }
}

} // namespace mt
