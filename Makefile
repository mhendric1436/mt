# Makefile for mt_core.hpp and mt_core_tests.cpp
#
# Usage:
#   make            # build and run tests
#   make build      # build test binary
#   make test       # build and run tests
#   make check      # syntax-check header and run tests
#   make format     # format source files with clang-format
#   make clean      # remove build artifacts
#
# Override examples:
#   make CXX=clang++
#   make CXXFLAGS="-std=c++20 -O0 -g -Wall -Wextra -pedantic"

CXX ?= c++
CLANG_FORMAT ?= clang-format

CXXFLAGS ?= -std=c++20 -O0 -g -Wall -Wextra -Wpedantic
CPPFLAGS ?= -I.
LDFLAGS  ?=
LDLIBS   ?=

BUILD_DIR := build
BUILD_STAMP := $(BUILD_DIR)/.dir
TEST_BIN  := $(BUILD_DIR)/mt_core_tests

CORE_HEADER := mt_core.hpp
CORE_HEADERS := \
	mt_json.hpp \
	mt_errors.hpp \
	mt_query.hpp \
	mt_collection.hpp \
	mt_types.hpp \
	mt_backend.hpp \
	mt_metadata_cache.hpp \
	mt_database.hpp \
	mt_transaction.hpp \
	mt_table.hpp \
	$(CORE_HEADER)
TEST_SRC    := mt_core_tests.cpp
HEADER_CHECK_SRC := mt_core.cpp
FORMAT_FILES := $(CORE_HEADERS) mt_memory_backend.hpp $(HEADER_CHECK_SRC) $(TEST_SRC)

.PHONY: all build test check header-check format clean rebuild print-config

all: test

build: $(TEST_BIN)

$(BUILD_STAMP):
	mkdir -p $(BUILD_DIR)
	touch $(BUILD_STAMP)

$(TEST_BIN): $(TEST_SRC) $(CORE_HEADERS) mt_memory_backend.hpp | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(TEST_SRC) -o $@ $(LDFLAGS) $(LDLIBS)

test: build
	./$(TEST_BIN)

check: header-check test

format:
	$(CLANG_FORMAT) -i $(FORMAT_FILES)

# Compile a small translation unit that includes public headers. This catches
# syntax and include-order issues without compiling #pragma once headers directly.
header-check: $(HEADER_CHECK_SRC) $(CORE_HEADERS) mt_memory_backend.hpp | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -DMT_HEADER_SYNTAX_CHECK -fsyntax-only $(HEADER_CHECK_SRC)

rebuild: clean build

clean:
	rm -rf $(BUILD_DIR)

print-config:
	@echo "CXX      = $(CXX)"
	@echo "CLANG_FORMAT = $(CLANG_FORMAT)"
	@echo "CPPFLAGS = $(CPPFLAGS)"
	@echo "CXXFLAGS = $(CXXFLAGS)"
	@echo "LDFLAGS  = $(LDFLAGS)"
	@echo "LDLIBS   = $(LDLIBS)"
