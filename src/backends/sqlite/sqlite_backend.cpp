#include "mt/backends/sqlite.hpp"

#include "sqlite_detail.hpp"

// -----------------------------------------------------------------------------
// SQLite backend implementation unit.
//
// The current public header intentionally remains dependency-free and contains
// the pre-implementation skeleton inline so the default core checks do not need
// to link the optional SQLite backend. Concrete SQLite code will move here as
// the backend is implemented behind the optional sqlite-check target.
// -----------------------------------------------------------------------------
