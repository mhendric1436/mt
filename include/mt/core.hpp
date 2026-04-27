#pragma once

// -----------------------------------------------------------------------------
// mt/core.hpp
//
// Backend-agnostic micro-transaction core library umbrella header.
//
// Concrete storage engines can include narrower headers such as mt/backend.hpp
// when they do not need the full typed table API.
// -----------------------------------------------------------------------------

#include "mt/backend.hpp"
#include "mt/collection.hpp"
#include "mt/database.hpp"
#include "mt/errors.hpp"
#include "mt/hash.hpp"
#include "mt/json.hpp"
#include "mt/metadata_cache.hpp"
#include "mt/query.hpp"
#include "mt/table.hpp"
#include "mt/transaction.hpp"
#include "mt/types.hpp"
