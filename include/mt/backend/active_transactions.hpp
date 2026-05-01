#pragma once

#include "mt/collection.hpp"

// -----------------------------------------------------------------------------
// mt/backend/active_transactions.hpp
//
// Backend active transaction metadata interface.
// -----------------------------------------------------------------------------

namespace mt
{

class IBackendActiveTransactions
{
  public:
    virtual ~IBackendActiveTransactions() = default;

    // Returns a backend-owned opaque transaction ID.
    virtual TxId create_transaction_id() = 0;

    // Records active transaction metadata for backend cleanup/retention.
    virtual void register_active_transaction(
        TxId tx_id,
        Version start_version
    ) = 0;

    // Removes active transaction metadata. Missing IDs should be tolerated.
    virtual void unregister_active_transaction(TxId tx_id) noexcept = 0;
};

} // namespace mt
