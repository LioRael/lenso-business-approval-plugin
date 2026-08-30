# Agent instructions

This repository owns the independent human business-decision primitive for
Lenso vNext.

- Keep one approval request and its terminal evidence as the owned aggregate.
- Do not turn this Plugin into a workflow engine, callback runner, form engine,
  or arbitrary script host.
- Author request/decision/cancellation/expiration authority as exact configured
  Plugin Instance keys. Never infer ambient authority from being able to call
  the Capability.
- Keep PostgreSQL private. App activation verifies an operator-managed schema
  and never creates or upgrades it.
- Preserve client-supplied stable request IDs, requester-scoped idempotency,
  pending-only terminal transitions, and one revision increment in the same
  transaction as terminal evidence.
- Capability Descriptor and JSON Schemas are authoritative. Regenerate the
  Rust projection with `lenso-contract-codegen`; never edit it manually.
- In a shared Lenso workspace, run Cargo through its `lenso-cargo` wrapper.
