# Lenso Business Approval Plugin

Independent durable human business decisions for Lenso applications.

This repository provides:

- `lenso.business-approval@1` with `request`, `decide`, `cancel`, `read`, and
  `expire` request Operations; and
- `lenso-business-approval-postgres-plugin`, which owns one private,
  operator-managed PostgreSQL schema.

## First slice

- Requesters supply a stable `request_id` and idempotency key.
- Idempotency is scoped to the exact requester Plugin Instance. An identical
  retry returns the existing approval; a changed intent fails closed.
- Each request begins at revision `1` in `pending`.
- Only `pending` may become `approved`, `rejected`, `cancelled`, or `expired`.
- The terminal status, caller Instance, actor/evidence reference, timestamp,
  and revision increment are committed in one transaction.
- Request, decision, cancellation, expiration, and read authority comes only
  from configured exact Plugin Instance keys. A Capability binding alone does
  not grant ambient authority.

The original requester Instance may cancel and read its own request. Configured
decider and expiration executor Instances may read across requesters; one
requester cannot read another requester's approval. Business Plugins retain
resource ownership, human eligibility, presentation, and all work triggered
after a decision.

## Operator setup

```rust,no_run
use lenso_business_approval_postgres_plugin::BusinessApprovalOperator;

# async fn setup(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
BusinessApprovalOperator::setup(database_url, "business_approval").await?;
# Ok(())
# }
```

Activation resolves only the configured database URL through the bound Secrets
Provider and verifies that the exact schema plan is already installed. It never
creates or upgrades database state.

## Focused verification

```sh
lenso-contract-codegen check \
  crates/lenso-capability-business-approval/capability.json \
  --rust crates/lenso-capability-business-approval/src/generated.rs
cargo check --locked --workspace --all-targets
cargo test --locked -p lenso-business-approval-postgres-plugin --lib
```

Optional PostgreSQL acceptance requires a disposable database whose name starts
with `lenso_business_approval_test`:

```sh
LENSO_BUSINESS_APPROVAL_TEST_DATABASE_URL=postgres://... \
  cargo test --locked -p lenso-business-approval-postgres-plugin \
  --features postgres-acceptance
```

## Deliberate non-goals

This is not a workflow engine. It does not execute callbacks, scripts, forms,
arbitrary payload-defined steps, routing graphs, notifications, or post-decision
business actions.
