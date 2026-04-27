#pragma once

#include "mt/backend.hpp"
#include "mt/errors.hpp"
#include "mt/metadata_cache.hpp"

#include <memory>
#include <utility>

// -----------------------------------------------------------------------------
// mt/database.hpp
//
// Database facade shared by transaction and table providers.
// -----------------------------------------------------------------------------

namespace mt
{

class TransactionProvider;
class TableProvider;
class Transaction;

template <class Row, class Mapping> class Table;

class Database
{
  public:
    explicit Database(std::shared_ptr<IDatabaseBackend> backend)
        : backend_(std::move(backend))
    {
        if (!backend_)
        {
            throw BackendError("Database requires a non-null backend");
        }
        backend_->bootstrap(BootstrapSpec{});
    }

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

  private:
    friend class TransactionProvider;
    friend class TableProvider;

    template <class Row, class Mapping> friend class Table;

    IDatabaseBackend& backend()
    {
        return *backend_;
    }

    const IDatabaseBackend& backend() const
    {
        return *backend_;
    }

    MetadataCache& metadata()
    {
        return metadata_;
    }

  private:
    std::shared_ptr<IDatabaseBackend> backend_;
    MetadataCache metadata_;
};

} // namespace mt
