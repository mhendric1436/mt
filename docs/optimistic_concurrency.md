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
set along with its observed version. At commit, each entry is checked against the
current backend state:

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
set of key/version pairs returned at snapshot time. At commit, the query is re-executed
against current backend metadata and the result set is compared.

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

## Building a Distributed Systems Stack

OCC with snapshot reads, predicate validation, and unique indexes composes into a
surprisingly complete set of coordination primitives. The patterns below show how `mt`
can serve as the transactional backbone for the building blocks of a distributed platform
— control planes, data planes, queuing, scheduling, orchestration, and beyond.

The unifying idea is that the mt store becomes the single source of truth for all
coordination state. Workers and services are stateless; they read current state, make a
local decision, and write the outcome atomically. If a concurrent actor made a
conflicting decision first, the conflict is detected at commit rather than discovered
later through inconsistency.

### Control Plane

Control planes manage the desired and observed state of a distributed system: cluster
membership, resource allocation, configuration, and the reconciliation loop that drives
actual state toward desired state.

**Leader election.** One process must own each reconciliation domain at a time. Model
ownership as a lease record with an expiry. A candidate reads the lease (absent-key read
if none exists), checks whether it is expired, and writes itself as the new owner:

```cpp
txs.run([&](mt::Transaction& tx)
{
    auto lease = leases.get(tx, domain_id);
    if (lease && lease->expires_at > now())
    {
        return; // still held
    }
    leases.put(tx, Lease{.key = domain_id, .owner = self_id,
                         .expires_at = now() + lease_duration});
});
```

The absent-key or version read ensures two candidates cannot both observe the lease as
expired and both succeed. Only the first to commit wins; the second hits point read
validation and retries, finding the lease now held.

**Resource allocation.** A scheduler reads current allocations across nodes, picks a
placement, and writes the assignment. A predicate read on the relevant node's allocation
list ensures no concurrent assignment shifted the available capacity between the read and
the write:

```cpp
txs.run([&](mt::Transaction& tx)
{
    auto allocs = allocations.query(tx, QuerySpec::key_prefix("node:" + node_id + ":"));
    auto used = total_cpu(allocs);
    if (used + requested > capacity)
    {
        throw InsufficientCapacity{};
    }
    allocations.put(tx, new_allocation);
});
```

The predicate read on the prefix protects against over-allocation: if another scheduler
placed a workload on the same node concurrently, the result set changes and the commit
fails.

**Configuration management.** A control plane propagates configuration to agents by
writing versioned config documents. Agents read the config at snapshot and act on a
consistent view. A config publisher uses read-modify-write to update a config atomically:

```cpp
txs.retry([&](mt::Transaction& tx)
{
    auto cfg = configs.require(tx, config_key);
    cfg.value = updated_value;
    cfg.generation += 1;
    configs.put(tx, cfg);
});
```

The point read on the config record means two concurrent publishers cannot both commit
conflicting updates without one being rejected and retried against the latest state.

### Data Plane

Data planes handle high-throughput request processing and need consistent state for
routing, policy enforcement, and admission control.

**Routing table updates.** Routes are written in batches and must be consistent: a
request must not be sent to a destination that has been removed in the same logical
update as the addition of its replacement. A single transaction writes the full set of
additions and removals atomically. Readers snapshot the routing table at a consistent
version and see either the old or new set, never a mix.

**Feature flag rollout.** Flags and their targeting rules are stored as documents. A
rollout transaction reads the current flag state, applies an incremental percentage
change, and writes the result. The point read ensures two concurrent rollout operations
do not interleave:

```cpp
txs.retry([&](mt::Transaction& tx)
{
    auto flag = flags.require(tx, flag_key);
    flag.rollout_pct = std::min(flag.rollout_pct + increment, 100);
    flags.put(tx, flag);
});
```

**Admission control.** A rate limiter tracks a token count. Each request transaction
reads the current tokens, checks the limit, and decrements atomically. Under high
concurrency the write-conflict validation ensures decrements are serialized; excess
requests get `TransactionConflict` and are rejected or queued rather than silently
over-admitted.

### Queue

A transactional store makes a durable, exactly-once queue without a separate message
broker.

**Enqueue.** A producer inserts a message with a unique message ID. A unique index on the
message ID field rejects duplicate enqueues at the backend level. The enqueue transaction
and any producer-side state update (marking a record as "pending delivery", for example)
commit atomically:

```cpp
txs.run([&](mt::Transaction& tx)
{
    orders.put(tx, order);                        // create the order
    queue.put(tx, Message{.id = order.id,         // enqueue atomically
                          .payload = serialize(order)});
});
```

If the transaction fails, neither the order nor the message is written. There is no
partial state to clean up.

**Claim and process.** A consumer queries for unclaimed messages, claims one with a
conditional write, processes it, and removes it — all protected by predicate and point
read validation:

```cpp
txs.run([&](mt::Transaction& tx)
{
    auto pending = queue.query(tx, QuerySpec::where_json_eq("$.status", Json("pending")));
    if (pending.empty())
    {
        return;
    }
    auto msg = pending.front();
    msg.status = "claimed";
    msg.worker = self_id;
    msg.claimed_at = now();
    queue.put(tx, msg);
});
```

The predicate read on the pending query ensures two workers cannot claim the same message
from the same result set observation. The first commit wins; the second retries and finds
the message already claimed.

**Exactly-once delivery.** Combine a unique index on a `delivery_id` field with a
conditional insert on the processing result. The consumer writes its output and marks the
message as processed in the same transaction. A duplicate redelivery attempt hits the
unique index constraint and fails cleanly, preventing double-processing.

**Visibility timeout.** A claimed message has a `claimed_until` timestamp. A reaper
periodically queries for messages with expired claims and resets them to `pending`. The
point read on each message's version ensures the reaper does not race with a worker that
is actively processing and re-claiming the same message.

### Job Scheduler

A scheduler distributes work units across a pool of workers, tracks their progress, and
handles failures.

**Exactly-once job dispatch.** A dispatcher queries for ready jobs (those with all
prerequisites complete and no current owner), claims one, and writes the worker
assignment. The predicate read on the ready-job query prevents two dispatchers from
seeing the same job as unclaimed and both writing an assignment:

```cpp
txs.run([&](mt::Transaction& tx)
{
    auto ready = jobs.query(tx, QuerySpec::where_json_eq("$.status", Json("ready")));
    if (ready.empty())
    {
        return;
    }
    auto job = ready.front();
    job.status = "running";
    job.worker = self_id;
    job.started_at = now();
    jobs.put(tx, job);
});
```

**Lease-based heartbeat.** A running job holds a lease. The worker renews the lease
periodically with a read-modify-write. A watchdog reads the lease and marks the job as
failed if the `last_heartbeat` is stale. Both paths are protected by point read
validation: if the worker and watchdog race, only one wins and the other retries with
the current state.

**Prerequisite tracking.** A job becomes ready when all its predecessor jobs have
completed. A completion transaction writes the finished job's status and queries all
successors to check whether their remaining prerequisites are now zero. The predicate
read on the prerequisite query ensures the successor's readiness decision is made against
a consistent view. If two concurrent job completions finish the last two prerequisites of
the same successor, only one will successfully write it as `ready`.

**Idempotent retry.** Jobs that fail can be retried. Each attempt has a unique
`attempt_id`. A unique index on `attempt_id` guarantees that even if a dispatch message
is delivered more than once, only one execution attempt commits successfully. The attempt
record and the job state transition commit in the same transaction.

### Workflow Orchestration

Workflows are long-running processes with multiple steps, branching logic, and failure
recovery. OCC enables a state machine model where every transition is a serializable,
conflict-detected commit.

**State machine transitions.** Each workflow instance has a current state. A step
executor reads the current state, validates the transition is legal for that state, and
writes the new state with its output:

```cpp
txs.run([&](mt::Transaction& tx)
{
    auto wf = workflows.require(tx, workflow_id);
    if (wf.state != "awaiting_approval")
    {
        throw InvalidTransition{};
    }
    wf.state = "approved";
    wf.approved_by = approver_id;
    wf.approved_at = now();
    workflows.put(tx, wf);
});
```

The point read on the workflow record ensures that two concurrent approval attempts
cannot both succeed. The second commit fails validation and the caller can surface a
conflict error to the user.

**Fan-out and join.** A workflow step spawns N parallel sub-tasks and then waits for all
to complete. When a sub-task finishes, it writes its result and queries the sibling
results to check whether all are done. The predicate read on the sibling query ensures
that the join decision — transitioning the parent workflow to its next state — is made
against a consistent view of all sibling outcomes. Two sub-tasks completing simultaneously
will race, but only one will successfully write the parent's next-state transition.

**Saga coordination.** A saga executes a sequence of steps across multiple services, with
compensating actions for rollback. Each step records its forward action and its
compensation in the same transaction. A saga coordinator queries incomplete steps to
decide whether to proceed or compensate. Predicate reads on the step records protect the
coordinator's decision against concurrent step execution or compensating transactions
modifying the same saga.

**Durable timers.** Workflow timeouts and scheduled step triggers are stored as timer
documents with a `fire_at` timestamp. A timer worker queries for timers past their
deadline, claims one, and fires the associated event in a single transaction. The claim
is protected by the same predicate read pattern as queue consumers — two timer workers
cannot both fire the same timer.

### Distributed Locks and Leases

Locks and leases are the primitive coordination unit for everything above. The conditional
insert pattern generalizes to any exclusive resource:

```cpp
// Acquire
txs.run([&](mt::Transaction& tx)
{
    if (locks.get(tx, lock_key))
    {
        throw LockHeld{};
    }
    locks.put(tx, Lock{.key = lock_key, .holder = self_id,
                       .expires_at = now() + ttl});
});

// Renew
txs.retry([&](mt::Transaction& tx)
{
    auto lock = locks.require(tx, lock_key);
    if (lock.holder != self_id)
    {
        throw LockLost{};
    }
    lock.expires_at = now() + ttl;
    locks.put(tx, lock);
});
```

The absent-key read on acquire and the point read on renew give the same safety guarantee
as a compare-and-swap: only the holder can renew, and expiry plus re-acquire is conflict-
detected. No external lock manager is required.

### Inventory and Reservation

Any system that tracks a finite resource — seats, stock, quota, capacity — follows the
same read-check-decrement pattern. OCC makes the check and the decrement atomic:

```cpp
txs.run([&](mt::Transaction& tx)
{
    auto item = inventory.require(tx, sku);
    if (item.quantity < requested)
    {
        throw OutOfStock{};
    }
    item.quantity -= requested;
    inventory.put(tx, item);

    cart.put(tx, CartItem{.sku = sku, .qty = requested, .user = user_id});
});
```

Two concurrent buyers for the last unit both read `quantity == 1`. One commits; the
other hits point read validation, retries, reads `quantity == 0`, and surfaces the
out-of-stock condition. No oversell is possible without a separate serialization
mechanism.

### Audit Log and Event Sourcing

Every committed transaction writes an immutable history row to the backend alongside the
current-state upsert. For systems that need a full event log, each state-changing
transaction can also write an explicit event document in the same atomic commit:

```cpp
txs.run([&](mt::Transaction& tx)
{
    auto acct = accounts.require(tx, account_id);
    acct.balance -= amount;
    accounts.put(tx, acct);

    events.put(tx, Event{.id = new_event_id(), .kind = "debit",
                         .account = account_id, .amount = amount,
                         .balance_after = acct.balance});
});
```

Because the state change and the event record commit together, the event log is
guaranteed to be consistent with the current state — no committed state change exists
without its corresponding event, and no event exists for a transaction that was rolled
back. A unique index on `event_id` provides idempotency for retried event writes.

### Service Registry and Health Tracking

A service registry stores the live set of service instances. Each instance registers
itself on startup by writing a record with a `last_seen` timestamp and renews it
periodically with a heartbeat. A health monitor queries for instances with stale
heartbeats and removes them:

```cpp
// Registration and heartbeat (same shape)
txs.retry([&](mt::Transaction& tx)
{
    auto reg = registry.get(tx, instance_key);
    if (reg && reg->generation != expected_generation)
    {
        throw RegistrationConflict{};
    }
    registry.put(tx, Registration{.key = instance_key, .addr = self_addr,
                                   .last_seen = now(), .generation = new_gen});
});

// Reaper
txs.run([&](mt::Transaction& tx)
{
    auto stale = registry.query(tx, QuerySpec::key_prefix("svc:" + svc_name + ":"));
    for (const auto& r : stale)
    {
        if (r.last_seen < now() - timeout)
        {
            registry.erase(tx, r.key);
        }
    }
});
```

The predicate read on the reaper's query and the point read on each instance record
protect against racing with a concurrent heartbeat. An instance that renews itself while
the reaper is evaluating it will cause the reaper's commit to fail, preventing a live
instance from being incorrectly evicted.

### Composing Primitives

The patterns above are not independent. A real platform composes them: the job scheduler
uses leases for heartbeats, the queue uses the reservation pattern for claiming, the
workflow engine uses state machine transitions for step progression and fan-out/join for
synchronization, and all of them use the audit log pattern for observability.

Because all coordination state lives in the same transactional store, cross-cutting
concerns — changing a job's owner and enqueuing a notification in the same transaction,
or completing a saga step and decrementing a quota atomically — require no two-phase
commit or distributed transaction protocol. The mt commit sequence handles it.
