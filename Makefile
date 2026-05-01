# Makefile for mt public headers and tests
#
# Usage:
#   make            # build and run tests
#   make build      # build test binary
#   make test       # build and run tests
#   make check      # syntax-check header and run tests
#   make sqlite-check # build and run optional SQLite backend tests
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

BUILD_DIR := build
BUILD_STAMP := $(BUILD_DIR)/.dir
TEST_BIN  := $(BUILD_DIR)/mt_core_tests
CODEGEN_TEST_BIN := $(BUILD_DIR)/mt_codegen_tests
SQLITE_TEST_BIN := $(BUILD_DIR)/sqlite_backend_tests
GENERATED_DIR := $(BUILD_DIR)/generated

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
	include/mt/metadata_cache.hpp \
	include/mt/database.hpp \
	include/mt/transaction.hpp \
	include/mt/table.hpp \
	include/mt/backends/memory.hpp \
	include/mt/backends/sqlite.hpp \
	include/mt/backends/postgres.hpp \
	$(CORE_HEADER)
TEST_SRC    := tests/mt_core_tests.cpp
CODEGEN_TEST_SRC := tests/mt_codegen_tests.cpp
SQLITE_BACKEND_SRC := \
	src/backends/sqlite/sqlite_backend.cpp \
	src/backends/sqlite/sqlite_constraints.cpp \
	src/backends/sqlite/sqlite_document.cpp \
	src/backends/sqlite/sqlite_schema.cpp \
	src/backends/sqlite/sqlite_session.cpp \
	src/backends/sqlite/sqlite_state.cpp
SQLITE_BACKEND_HEADERS := \
	src/backends/sqlite/sqlite_constraints.hpp \
	src/backends/sqlite/sqlite_detail.hpp \
	src/backends/sqlite/sqlite_document.hpp \
	src/backends/sqlite/sqlite_schema.hpp \
	src/backends/sqlite/sqlite_session.hpp \
	src/backends/sqlite/sqlite_state.hpp
SQLITE_TEST_SRC := $(wildcard tests/backends/sqlite/*.cpp)
SQLITE_TEST_HEADERS := $(wildcard tests/backends/sqlite/*.hpp)
HEADER_CHECK_SRC := src/mt_core.cpp
CODEGEN := python3 tools/mt_codegen.py
CODEGEN_VALIDATION_TEST := python3 tools/test_mt_codegen.py
EXAMPLE_SCHEMA := examples/schemas/user.mt.json
GENERATED_EXAMPLE_HEADER := $(GENERATED_DIR)/user.hpp
FORMAT_FILES := $(CORE_HEADERS) $(HEADER_CHECK_SRC) $(TEST_SRC) $(CODEGEN_TEST_SRC) $(wildcard $(SQLITE_BACKEND_SRC) $(SQLITE_BACKEND_HEADERS) $(SQLITE_TEST_SRC) $(SQLITE_TEST_HEADERS))
PUML_FILES := $(wildcard docs/*.puml)

.PHONY: all build test check sqlite-build sqlite-test sqlite-check codegen-examples codegen-validation header-check format docs-png clean-docs clean rebuild print-config

all: test

build: $(TEST_BIN) $(CODEGEN_TEST_BIN)

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

$(SQLITE_TEST_BIN): $(SQLITE_TEST_SRC) $(SQLITE_TEST_HEADERS) $(SQLITE_BACKEND_SRC) $(SQLITE_BACKEND_HEADERS) $(GENERATED_EXAMPLE_HEADER) $(CORE_HEADERS) | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(SQLITE_CFLAGS) $(CXXFLAGS) $(SQLITE_TEST_SRC) $(SQLITE_BACKEND_SRC) -o $@ $(LDFLAGS) $(SQLITE_LIBS) $(LDLIBS)

test: build
	./$(TEST_BIN)
	./$(CODEGEN_TEST_BIN)

check: codegen-examples codegen-validation header-check test

sqlite-build: $(SQLITE_TEST_BIN)

sqlite-test: sqlite-build
	./$(SQLITE_TEST_BIN)

sqlite-check: sqlite-test

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
