#pragma once

#include "mt/backends/memory/backend.hpp"

// -----------------------------------------------------------------------------
// mt/backends/memory.hpp
//
// Small process-local, non-durable backend for tests, local development, and
// application-owned ephemeral use cases.
//
// This is intentionally backend-compatible with mt/core.hpp and has no test-row
// knowledge. Tests can include this file instead of embedding a backend.
// -----------------------------------------------------------------------------
