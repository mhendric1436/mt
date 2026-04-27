#pragma once

// -----------------------------------------------------------------------------
// mt_core.hpp
//
// Backend-agnostic micro-transaction core library umbrella header.
//
// Concrete storage engines can include narrower headers such as mt_backend.hpp
// when they do not need the full typed table API.
// -----------------------------------------------------------------------------

#include "mt_backend.hpp"
#include "mt_collection.hpp"
#include "mt_database.hpp"
#include "mt_errors.hpp"
#include "mt_json.hpp"
#include "mt_metadata_cache.hpp"
#include "mt_query.hpp"
#include "mt_table.hpp"
#include "mt_transaction.hpp"
#include "mt_types.hpp"
