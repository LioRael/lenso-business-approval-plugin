# D1 Approval profile

The `lenso.business-approval.d1` Plugin supplies the existing
`lenso.business-approval@1` Role. Its configuration contains exact requester,
decider and expiration executor Instance keys. Its required private `store`
facility selects exactly one D1 binding and event scope, with configuration
`{ "profile": "workers-d1" }`. It never receives the complete Host environment.

The Owner `setup(database)` action is explicit. Runtime readiness verifies the
owner schema version and does not create or upgrade tables. Every finite read or transition starts a fresh
`withSession("first-primary")` session. A transition batch is atomic; a read
after an approval wait observes current primary state. The same Rust owner service validates
requests, caller authority, immutable intent digest, expiration, identifiers and
human scoped-child denial for PostgreSQL and D1 implementations. The backend
exposes only request, read, decide, cancel and expire internally, not raw SQL.

The initial pending aggregate has revision 1. A terminal transition uses one
conditional D1 batch (`status=pending AND revision=1`) to store evidence and
revision 2, then read its result. Request IDs and requester-scoped delivery keys
are unique. Shared Rust compares the complete original immutable intent on a
replay. Only a requester can cancel; only an expired pending request can expire.
An already terminal result cannot be changed by a later or competing decision.

Persisted expiration uses absolute UTC microseconds. Trusted Owner JavaScript
`Date.now()` supplies current wall time; request deadlines do not reuse a relative
Driver clock across deployments. Exceptions, lost replies, event closure or
malformed responses are Runtime unavailable because a transition may have
committed. Consumers read the original request instead of automatically deciding
again. Startup does not rewrite pending or terminal rows.

The admitted caller remains responsible for forwarding a freshly verified human
assertion. The Owner's sealed-context helper rejects delegated children and does
not replace remote realm verification or current CredentialState/RBAC checks.
The initial Workers Management profile uses human user API tokens; the separate
Native Account/Cookie/Human PAT lifecycle is not promoted to Workers support.
Node SQLite components are not a whole-App or workerd qualification receipt.
