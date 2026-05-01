#pragma once

#include "mt/backends/memory/backend.hpp"

// -----------------------------------------------------------------------------
// mt/backends/memory.hpp
//
// Small process-local, non-durable backend for tests, local development, and
// application-owned ephemeral use cases.
// Migration support is intentionally out of scope because memory schema and rows
// do not persist across process restarts.
//
// This is intentionally backend-compatible with mt/core.hpp and has no test-row
// knowledge. Tests can include this file instead of embedding a backend.
// User code should include this wrapper instead of the implementation subheaders
// under mt/backends/memory/.
// -----------------------------------------------------------------------------
