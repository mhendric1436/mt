# Makefile for mt_core.hpp and mt_core_tests.cpp
#
# Usage:
#   make            # build and run tests
#   make build      # build test binary
#   make test       # build and run tests
#   make check      # syntax-check header and run tests
#   make clean      # remove build artifacts
#
# Override examples:
#   make CXX=clang++
#   make CXXFLAGS="-std=c++20 -O0 -g -Wall -Wextra -pedantic"

CXX ?= c++

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

.PHONY: all build test check header-check clean rebuild print-config

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

# Compile the header as a translation unit to catch standalone header errors.
# -x c++ forces C++ parsing even though the file extension is .hpp.
header-check: $(CORE_HEADERS) | $(BUILD_STAMP)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -x c++ -fsyntax-only $(CORE_HEADER)

rebuild: clean build

clean:
	rm -rf $(BUILD_DIR)

print-config:
	@echo "CXX      = $(CXX)"
	@echo "CPPFLAGS = $(CPPFLAGS)"
	@echo "CXXFLAGS = $(CXXFLAGS)"
	@echo "LDFLAGS  = $(LDFLAGS)"
	@echo "LDLIBS   = $(LDLIBS)"
