# Business Approval v1 Plugin card

## Owner and deletion boundary

The PostgreSQL Plugin owns approval request identity, requester-scoped
idempotency, pending/terminal state, monotonic revision, and terminal decision
evidence. Removing its package, Instance, bindings, and schema removes approval
history without deleting the referenced business records or their workflows.

## Capability

- Provides portable `lenso.business-approval@1`.
- Requires exactly one `lenso.secrets@1` Provider for the database URL.
- `request` creates or idempotently resolves one request.
- `decide` records approved/rejected evidence.
- `cancel` is limited to the original requester Instance.
- `read` returns the durable request and terminal evidence.
- `expire` terminates a due request from an exact expiration executor Instance.

## Authorization

Configuration supplies exact requester, decider, and expiration executor
Plugin Instance allowlists. A requester reads only requests it created; decider
and expiration executor Instances may read across requesters. The Provider
checks `InvocationContext.caller_instance()` and applies requester ownership in
the database query; no ambient Host, Capability, process, or network authority
is inferred.

The configured caller attests the opaque human actor reference supplied to
`request`, `decide`, or `cancel`. Human identity verification and business
eligibility remain with the calling Plugin and its Auth boundary.

## State and lifecycle

Configuration also supplies an owned schema and database URL secret reference.
Activation resolves the secret and verifies an already-installed schema.
Deactivation closes the pool. Setup and upgrade remain explicit operator work.

Each aggregate is created at revision `1`. A row lock serializes terminal
transitions. The first valid terminal transition writes its evidence and moves
to revision `2`; every later terminal attempt returns `already_terminal`.

## Honest limits

There is no workflow graph, callback delivery, arbitrary JSON form/payload,
script execution, notification delivery, decision delegation, quorum, or Audit
outbox in v1. The Plugin does not determine who is an eligible human approver;
the configured decider Instance owns that policy.
