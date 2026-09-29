# Immutable management approval intent

Business Approval 1.1 adds an optional SHA-256 `intent_digest` to request and
read. The digest is stored with the request, participates in requester-scoped
same-intent detection, and cannot be changed by a later decision. Reusing a key
with another digest conflicts. `management-operation` subjects require a digest
at admission. A durable Management operation reference is the subject ID; the
journal owns the full canonical input and exact target, operation/schema,
deployment, actor and resource-version binding. No sensitive input is copied
into this Plugin or debug output.

Existing requests remain compatible without the optional field. Operators must
run `BusinessApprovalOperator::upgrade` to install migration 2 before activating
an existing deployment. Activation verifies the managed ledger and never
migrates. The migration adds one nullable, format-constrained column; it does
not change or retroactively approve old records. Old management requests lacking
a digest are not valid execution evidence.

Approval still owns one decision, rather than human permission or dispatch.
The exact decider caller must verify the current person's identity and decide
permission, enforce the product's self-approval policy, and retain evidence.
Management rechecks current credential, permission, digest, deployment, expiry
and resource version before execution. Approval never executes a handler and
an approved status never proves that a business operation completed.
Existing pending-only CAS, decision-time expiry and requester isolation remain
unchanged. Native PostgreSQL is validated. Workers PG/Hyperdrive or authenticated
remote approval topology require separate qualification.

## Verification

```sh
lenso-contract-codegen workspace check --manifest-path Cargo.toml
LENSO_BUSINESS_APPROVAL_TEST_DATABASE_URL=postgresql://.../lenso_business_approval_test_security \
  cargo test --locked -p lenso-business-approval-postgres-plugin --features postgres-acceptance
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

The real PostgreSQL test proves persisted digest reads and changed-digest replay
conflicts alongside the existing terminal decision/restart semantics. Candidate
CI runs on an immutable `candidate/**` revision before a normal fast-forward;
publication and deployment are separate actions.
