# Lenso Business Approval context

`lenso-business-approval-plugin` owns one narrow primitive: durable requests
for a human business decision and the evidence of their single terminal
transition.

`lenso.business-approval@1` lets trusted requester Instances create an approval
with a stable client-supplied request ID and requester-scoped idempotency key.
Trusted decision Instances approve or reject it, its original requester
Instance may cancel it, trusted expiration executor Instances may expire it
after its deadline, and those configured Instance sets may read it.

The Plugin stores only opaque business references, actor references, decision
evidence references, status, timestamps, and revision. The calling business
Plugin owns the resource, presentation, eligibility rules, and work performed
after a decision. No callback, script, form, DAG, or workflow execution is
owned here.

Every terminal transition locks the request row, requires `pending`, writes
terminal evidence, and increments the revision in one PostgreSQL transaction.
