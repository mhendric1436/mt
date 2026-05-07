# Optimistic Concurrency Control

`mt` uses optimistic concurrency control (OCC). Transactions read freely from a
consistent snapshot and buffer all writes locally. At commit time, the core validates
that the data the transaction relied on has not changed. If it has, the transaction
throws `TransactionConflict` and the caller can retry with fresh data.

This model avoids holding locks during transaction execution. Concurrent non-conflicting
transactions — the common case in most workloads — commit without interference.
Conflicts are only paid for when they actually occur.

## Snapshot Reads

When a transaction begins, the backend clock is read and stored as `start_version`.
Every read for the lifetime of the transaction is served from that snapshot: the
transaction sees only commits that completed before it started, and it never sees
partial writes from concurrent in-flight transactions.

```text
T=0: tx A begins, start_version = 5
T=1: tx B commits key "user:1" at version 6
T=2: tx A reads "user:1" — still sees version 5 snapshot, not B's write
T=3: tx A commits — point read validation fails because "user:1" changed
```

The snapshot is fixed at `begin()` and does not advance while the transaction runs.

## Read-Your-Writes

Buffered writes are visible to subsequent reads within the same transaction. If a
transaction writes a key and then reads it back, it receives the locally buffered value
rather than the snapshot. The same applies to `list` and `query`: pending writes in the
transaction's write set are overlaid on snapshot results before returning.

```cpp
users.put(tx, alice);              // buffered locally
auto row = users.get(tx, "alice"); // returns the local write, not the snapshot
```

Deletes within a transaction are also reflected immediately: an erased key is absent
from subsequent reads, lists, and queries within the same transaction.

## Three-Phase Commit Validation

At commit, the core acquires the clock lock and runs three validations before writing
anything to storage.

### Point Read Validation

Every key fetched with `get` or `require` during the transaction is tracked in the read
set along with its observed version and content hash. At commit, each entry is checked
against the current backend state:

- If the key was absent when read (`observed_version == 0`), any concurrent commit that
  created that key after `start_version` is a conflict.
- If the key was present, the current version must exactly match the observed version. A
  different version — whether updated or deleted — is a conflict.

Reading a missing key counts. A transaction that read a non-existent key and is about to
create it will conflict if another transaction created the same key first.

### Write Conflict Validation

Keys that were written but never read by the same transaction are also protected. For
each write-only key, the core checks whether any commit has touched that key since
`start_version`. This prevents lost updates when a transaction writes a document without
reading it first.

### Predicate Read Validation

`list` and `query` calls record a predicate read: the query parameters plus the sorted
set of (key, version, hash) tuples returned at snapshot time. At commit, the query is
re-executed against current backend metadata and the result set is compared.

If the membership or state of any row in the result has changed — new rows appeared,
existing rows changed, or rows were deleted — the commit throws `TransactionConflict`.
This prevents phantom anomalies: a transaction that made a decision based on the full
result of a query is invalidated if that result would differ at commit time.

## Retrying Conflicts

`TransactionProvider::retry()` re-runs the transaction body from a fresh snapshot on
`TransactionConflict`. Each attempt starts a new transaction with a new `start_version`,
so it sees all commits that occurred since the last attempt.

```cpp
txs.retry([&](mt::Transaction& tx)
{
    auto user = users.require(tx, "user:1");
    user.score += 1;
    users.put(tx, user);
});
```

`retry()` is appropriate when the transaction body is safe to run more than once.
Operations with non-idempotent external side effects — sending email, charging a payment,
publishing a message — must not be placed inside a retry body. Handle those at a higher
level after the transaction commits.

The default `RetryPolicy` allows three attempts. A custom policy can be passed to
`retry(policy, fn)`.

## Interesting Use Cases

### Conditional Insert

A transaction that creates a document only if it does not already exist:

```cpp
txs.run([&](mt::Transaction& tx)
{
    if (users.get(tx, "user:1"))
    {
        return; // already exists
    }
    users.put(tx, new_user);
});
```

Reading the absent key records `observed_version == 0` in the read set. If a concurrent
transaction inserts `"user:1"` before this commit, point read validation catches the
newly created key and throws `TransactionConflict`. The second transaction either retries
and skips the insert or takes a different branch.

### Read-Modify-Write

Incrementing a counter or updating a field based on its current value:

```cpp
txs.retry([&](mt::Transaction& tx)
{
    auto account = accounts.require(tx, account_id);
    if (account.balance < amount)
    {
        throw InsufficientFunds{};
    }
    account.balance -= amount;
    accounts.put(tx, account);
});
```

The `require` call records the account's version. If another transaction debits the same
account concurrently, point read validation detects the version change and forces a
retry. The retry re-reads the current balance and re-evaluates the guard.

### Multi-Key Atomic Transfer

Moving a value between two documents atomically:

```cpp
txs.retry([&](mt::Transaction& tx)
{
    auto src = accounts.require(tx, from_id);
    auto dst = accounts.require(tx, to_id);

    src.balance -= amount;
    dst.balance += amount;

    accounts.put(tx, src);
    accounts.put(tx, dst);
});
```

Both reads enter the read set. A concurrent transaction touching either account
invalidates this one. Because the snapshot is fixed, the invariant `src.balance +
dst.balance == constant` is evaluated against a consistent view, not a mix of versions.

### Phantom Prevention

A transaction that counts active sessions and rejects a new login if a limit is reached:

```cpp
txs.run([&](mt::Transaction& tx)
{
    auto active = sessions.query(tx, QuerySpec::key_prefix("session:user:42:"));
    if (active.size() >= max_sessions)
    {
        throw SessionLimitExceeded{};
    }
    sessions.put(tx, new_session);
});
```

The `query` call records the full result set as a predicate read. If a concurrent
transaction creates another session for the same user before this commit, predicate
validation detects the new row and throws `TransactionConflict`. Without predicate reads,
two concurrent transactions could both count `active.size() < max_sessions` and both
proceed, exceeding the limit.

### Unique Slot Claim

Reserving a named slot — a username, a time slot, a ticket — where exactly one
transaction must succeed:

```cpp
txs.run([&](mt::Transaction& tx)
{
    if (reservations.get(tx, slot_key))
    {
        throw SlotTaken{};
    }
    reservations.put(tx, Reservation{.key = slot_key, .owner = requester});
});
```

The absent-key read records `observed_version == 0`. Two concurrent transactions
targeting the same slot can both pass the `get` check against their respective snapshots,
but only one will win at commit. The second commit fails point read validation because
the first commit created the key after `start_version`.

For fields that must be unique across all rows — such as an email address — a unique
index provides an additional backend-enforced guard that complements OCC validation.

### Consistent Aggregate Read

Reading multiple tables to build a consistent view for display or reporting:

```cpp
auto tx = txs.begin();
auto order = orders.require(tx, order_id);
auto items = order_items.query(tx, QuerySpec::key_prefix("item:" + order_id + ":"));
auto customer = customers.require(tx, order.customer_id);
tx.commit();
```

All three reads are served from the same `start_version` snapshot. The result is
internally consistent: the items and customer match the order state at a single point in
time, not a mix of before and after some concurrent update.

## When OCC Works Well

OCC performs best when conflicts are rare — distinct transactions typically access
different keys or non-overlapping result sets. Read-heavy workloads with infrequent
writes to the same keys are a natural fit.

OCC is less suitable when:

- Many concurrent transactions contend on a small number of hot keys (a shared counter
  that every request increments, for example). Retries accumulate under high write
  contention, and a pessimistic lock may be cheaper.
- A transaction must guarantee forward progress despite contention. `retry()` gives up
  after `max_attempts` and re-throws `TransactionConflict`; callers that need guaranteed
  progress must handle that case or use external coordination.
- A transaction performs expensive work before the conflict is discovered at commit.
  Detecting the conflict earlier — by reading contested keys first — reduces wasted work.
