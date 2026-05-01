#include "sqlite_constraints.hpp"

#include "sqlite_document.hpp"
#include "sqlite_schema.hpp"

#include "mt/errors.hpp"
#include "mt/query.hpp"

namespace mt::backends::sqlite
{

void check_unique_constraints(
    detail::Connection& connection,
    CollectionId collection,
    const WriteEnvelope& write
)
{
    if (write.kind == WriteKind::Delete)
    {
        return;
    }

    for (const auto& index : load_collection_indexes(connection, collection))
    {
        if (!index.unique)
        {
            continue;
        }

        auto write_value = json_path_value(write.value, index.json_path);
        if (!write_value)
        {
            continue;
        }

        detail::Statement statement{
            connection.get(), detail::PrivateSchemaSql::select_current_unique_index_candidates()
        };
        statement.bind_int64(1, collection);
        statement.bind_text(2, write.key);

        while (statement.step())
        {
            if (statement.column_is_null(1))
            {
                continue;
            }

            auto current_value =
                json_path_value(parse_stored_json(statement.column_text(1)), index.json_path);
            if (current_value && *current_value == *write_value)
            {
                throw BackendError("sqlite backend unique index constraint violation");
            }
        }
    }
}

} // namespace mt::backends::sqlite
