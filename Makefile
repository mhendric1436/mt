# Makefile for mt public headers and tests
#
# Usage:
#   make            # build and run tests
#   make build      # build test binary
#   make test       # build and run tests
#   make check      # syntax-check header and run tests
#   make memory-check # build and run memory backend tests
#   make sqlite-check # build and run optional SQLite backend tests
#   make postgres-check # run optional PostgreSQL backend tests when configured
#   make format     # format source files with clang-format
#   make docs-png   # generate PNG diagrams from PlantUML files
#   make clean-docs # remove generated PlantUML PNG files
#   make clean      # remove build artifacts
#
# Override examples:
#   make CXX=clang++
#   make CXXFLAGS="-std=c++20 -O0 -g -Wall -Wextra -pedantic"

CXX ?= c++
CLANG_FORMAT ?= clang-format
PKG_CONFIG ?= pkg-config
PLANTUML ?= plantuml

CXXFLAGS ?= -std=c++20 -O0 -g -Wall -Wextra -Wpedantic
CPPFLAGS ?= -Iinclude
LDFLAGS  ?=
LDLIBS   ?=
SQLITE_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags sqlite3 2>/dev/null)
SQLITE_LIBS ?= $(shell $(PKG_CONFIG) --libs sqlite3 2>/dev/null)
SQLITE_LIBS := $(if $(SQLITE_LIBS),$(SQLITE_LIBS),-lsqlite3)
POSTGRES_CFLAGS ?= $(shell $(PKG_CONFIG) --cflags libpq 2>/dev/null)
POSTGRES_LIBS ?= $(shell $(PKG_CONFIG) --libs libpq 2>/dev/null)

BUILD_DIR := build
BUILD_STAMP := $(BUILD_DIR)/.dir
TEST_BIN  := $(BUILD_DIR)/mt_core_tests
CODEGEN_TEST_BIN := $(BUILD_DIR)/mt_codegen_tests
MEMORY_TEST_BIN := $(BUILD_DIR)/memory_backend_tests
SQLITE_TEST_BIN := $(BUILD_DIR)/sqlite_backend_tests
POSTGRES_TEST_BIN := $(BUILD_DIR)/postgres_backend_tests
GENERATED_DIR := $(BUILD_DIR)/generated
BACKEND_TEST_HEADERS := $(wildcard tests/backends/*.hpp)
BACKEND_HEADERS := $(wildcard include/mt/backend/*.hpp)
MEMORY_BACKEND_HEADERS := $(wildcard include/mt/backends/memory/*.hpp)

CORE_HEADER := include/mt/core.hpp
CORE_HEADERS := \
	include/mt/json.hpp \
	include/mt/json_parser.hpp \
	include/mt/hash.hpp \
	include/mt/errors.hpp \
	include/mt/query.hpp \
	include/mt/collection.hpp \
	include/mt/schema.hpp \
	include/mt/types.hpp \
	include/mt/backend.hpp \
	$(BACKEND_HEADERS) \
	include/mt/metadata_cache.hpp \
	include/mt/database.hpp \
	include/mt/transaction.hpp \
	include/mt/table.hpp \
	include/mt/backends/memory.hpp \
	$(MEMORY_BACKEND_HEADERS) \
	include/mt/backends/sqlite.hpp \
	include/mt/backends/postgres.hpp \
	$(CORE_HEADER)
TEST_SRC    := tests/mt_core_tests.cpp
CODEGEN_TEST_SRC := tests/mt_codegen_tests.cpp
MEMORY_TEST_SRC := $(wildcard tests/backends/memory/*.cpp)
MEMORY_TEST_HEADERS := $(wildcard tests/backends/memory/*.hpp)
SQLITE_BACKEND_SRC := $(wildcard src/backends/sqlite/*.cpp)
SQLITE_BACKEND_HEADERS := $(wildcard src/backends/sqlite/*.hpp)
SQLITE_TEST_SRC := $(wildcard tests/backends/sqlite/*.cpp)
SQLITE_TEST_HEADERS := $(wildcard tests/backends/sqlite/*.hpp)
POSTGRES_BACKEND_SRC := $(wildcard src/backends/postgres/*.cpp)
POSTGRES_BACKEND_HEADERS := $(wildcard src/backends/postgres/*.hpp)
POSTGRES_TEST_SRC := $(wildcard tests/backends/postgres/*.cpp)
POSTGRES_TEST_HEADERS := $(wildcard tests/backends/postgres/*.hpp)
HEADER_CHECK_SRC := src/mt_core.cpp
CODEGEN := python3 tools/mt_codegen.py
CODEGEN_VALIDATION_TEST := python3 tools/test_mt_codegen.py
EXAMPLE_SCHEMA := examples/schemas/user.mt.json
GENERATED_EXAMPLE_HEADER := $(GENERATED_DIR)/user.hpp
FORMAT_FILES := $(CORE_HEADERS) $(HEADER_CHECK_SRC) $(TEST_SRC) $(CODEGEN_TEST_SRC) $(BACKEND_TEST_HEADERS) $(MEMORY_TEST_SRC) $(MEMORY_TEST_HEADERS) $(SQLITE_BACKEND_SRC) $(SQLITE_BACKEND_HEADERS) $(SQLITE_TEST_SRC) $(SQLITE_TEST_HEADERS) $(POSTGRES_BACKEND_SRC) $(POSTGRES_BACKEND_HEADERS) $(POSTGRES_TEST_SRC) $(POSTGRES_TEST_HEADERS)
PUML_FILES := $(wildcard docs/*.puml)

.PHONY: all build test check memory-build memory-test memory-check sqlite-build sqlite-test sqlite-check postgres-build postgres-test postgres-check codegen-examples codegen-validation header-check format docs-png clean-docs clean rebuild print-config

all: test

build: $(TEST_BIN) $(CODEGEN_TEST_BIN) $(MEMORY_TEST_BIN)

$(BUILD_STAMP):
	mkdir -p $(BUILD_DIR)
	touch $(BUILD_STAMP)

$(TEST_BIN): $(TEST_SRC) $(CORE_HEADERS) | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(TEST_SRC) -o $@ $(LDFLAGS) $(LDLIBS)

$(GENERATED_DIR): | $(BUILD_STAMP)
	mkdir -p $(GENERATED_DIR)

$(GENERATED_EXAMPLE_HEADER): $(EXAMPLE_SCHEMA) tools/mt_codegen.py | $(GENERATED_DIR)
	$(CODEGEN) $(EXAMPLE_SCHEMA) -o $@

$(CODEGEN_TEST_BIN): $(CODEGEN_TEST_SRC) $(GENERATED_EXAMPLE_HEADER) $(CORE_HEADERS) | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CODEGEN_TEST_SRC) -o $@ $(LDFLAGS) $(LDLIBS)

$(MEMORY_TEST_BIN): $(MEMORY_TEST_SRC) $(MEMORY_TEST_HEADERS) $(BACKEND_TEST_HEADERS) $(GENERATED_EXAMPLE_HEADER) $(CORE_HEADERS) | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(MEMORY_TEST_SRC) -o $@ $(LDFLAGS) $(LDLIBS)

$(SQLITE_TEST_BIN): $(SQLITE_TEST_SRC) $(SQLITE_TEST_HEADERS) $(BACKEND_TEST_HEADERS) $(SQLITE_BACKEND_SRC) $(SQLITE_BACKEND_HEADERS) $(GENERATED_EXAMPLE_HEADER) $(CORE_HEADERS) | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(SQLITE_CFLAGS) $(CXXFLAGS) $(SQLITE_TEST_SRC) $(SQLITE_BACKEND_SRC) -o $@ $(LDFLAGS) $(SQLITE_LIBS) $(LDLIBS)

$(POSTGRES_TEST_BIN): $(POSTGRES_TEST_SRC) $(POSTGRES_TEST_HEADERS) $(BACKEND_TEST_HEADERS) $(POSTGRES_BACKEND_SRC) $(POSTGRES_BACKEND_HEADERS) $(GENERATED_EXAMPLE_HEADER) $(CORE_HEADERS) | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(POSTGRES_CFLAGS) $(CXXFLAGS) $(POSTGRES_TEST_SRC) $(POSTGRES_BACKEND_SRC) -o $@ $(LDFLAGS) $(POSTGRES_LIBS) $(LDLIBS)

test: build
	./$(TEST_BIN)
	./$(CODEGEN_TEST_BIN)
	./$(MEMORY_TEST_BIN)

check: codegen-examples codegen-validation header-check test

memory-build: $(MEMORY_TEST_BIN)

memory-test: memory-build
	./$(MEMORY_TEST_BIN)

memory-check: memory-test

sqlite-build: $(SQLITE_TEST_BIN)

sqlite-test: sqlite-build
	./$(SQLITE_TEST_BIN)

sqlite-check: sqlite-test

postgres-build: $(POSTGRES_TEST_BIN)

postgres-test: postgres-build
	./$(POSTGRES_TEST_BIN)

postgres-check:
ifndef MT_POSTGRES_TEST_DSN
	@echo "Skipping postgres-check: MT_POSTGRES_TEST_DSN is not set"
else ifeq ($(POSTGRES_LIBS),)
	@echo "postgres-check requires libpq pkg-config metadata; set PKG_CONFIG_PATH if libpq is installed outside the default search path"
	@exit 1
else
	$(MAKE) postgres-test
endif

codegen-examples: $(GENERATED_EXAMPLE_HEADER)

codegen-validation:
	$(CODEGEN_VALIDATION_TEST)

format:
	$(CLANG_FORMAT) -i $(FORMAT_FILES)

docs-png:
	$(PLANTUML) -tpng $(PUML_FILES)

clean-docs:
	rm -f docs/*.png

# Compile a small translation unit that includes public headers. This catches
# syntax and include-order issues without compiling #pragma once headers directly.
header-check: $(HEADER_CHECK_SRC) $(CORE_HEADERS) | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -DMT_HEADER_SYNTAX_CHECK -fsyntax-only $(HEADER_CHECK_SRC)

rebuild: clean build

clean:
	rm -rf $(BUILD_DIR)

print-config:
	@echo "CXX      = $(CXX)"
	@echo "CLANG_FORMAT = $(CLANG_FORMAT)"
	@echo "PKG_CONFIG = $(PKG_CONFIG)"
	@echo "PLANTUML = $(PLANTUML)"
	@echo "CODEGEN  = $(CODEGEN)"
	@echo "CODEGEN_VALIDATION_TEST = $(CODEGEN_VALIDATION_TEST)"
	@echo "CPPFLAGS = $(CPPFLAGS)"
	@echo "CXXFLAGS = $(CXXFLAGS)"
	@echo "LDFLAGS  = $(LDFLAGS)"
	@echo "LDLIBS   = $(LDLIBS)"
	@echo "SQLITE_CFLAGS = $(SQLITE_CFLAGS)"
	@echo "SQLITE_LIBS = $(SQLITE_LIBS)"
	@echo "POSTGRES_CFLAGS = $(POSTGRES_CFLAGS)"
	@echo "POSTGRES_LIBS = $(POSTGRES_LIBS)"
	@echo "MT_POSTGRES_TEST_DSN = $(MT_POSTGRES_TEST_DSN)"
