#include "mt/backends/sqlite.hpp"

#include <cassert>
#include <iostream>

#define EXPECT_FALSE(expr) assert(!(expr))

#define EXPECT_THROW_AS(statement, exception_type)                                                 \
    do                                                                                             \
    {                                                                                              \
        bool did_throw = false;                                                                    \
        try                                                                                        \
        {                                                                                          \
            statement;                                                                             \
        }                                                                                          \
        catch (const exception_type&)                                                              \
        {                                                                                          \
            did_throw = true;                                                                      \
        }                                                                                          \
        assert(did_throw && "expected exception not thrown");                                      \
    } while (false)

void test_sqlite_backend_skeleton_reports_no_capabilities()
{
    mt::backends::sqlite::SqliteBackend backend;
    auto capabilities = backend.capabilities();

    EXPECT_FALSE(capabilities.query.key_prefix);
    EXPECT_FALSE(capabilities.query.json_equals);
    EXPECT_FALSE(capabilities.query.json_contains);
    EXPECT_FALSE(capabilities.query.order_by_key);
    EXPECT_FALSE(capabilities.query.custom_ordering);

    EXPECT_FALSE(capabilities.schema.json_indexes);
    EXPECT_FALSE(capabilities.schema.unique_indexes);
    EXPECT_FALSE(capabilities.schema.migrations);
}

void test_sqlite_backend_skeleton_rejects_operations()
{
    mt::backends::sqlite::SqliteBackend backend;

    EXPECT_THROW_AS(backend.open_session(), mt::BackendError);
    EXPECT_THROW_AS(backend.bootstrap(mt::BootstrapSpec{}), mt::BackendError);
    EXPECT_THROW_AS(
        backend.ensure_collection(mt::CollectionSpec{.logical_name = "users"}), mt::BackendError
    );
    EXPECT_THROW_AS(backend.get_collection("users"), mt::BackendError);
}

int main()
{
    test_sqlite_backend_skeleton_reports_no_capabilities();
    test_sqlite_backend_skeleton_rejects_operations();

    std::cout << "All sqlite backend tests passed.\n";
    return 0;
}
