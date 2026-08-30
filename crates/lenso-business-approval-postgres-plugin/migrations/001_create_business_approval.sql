CREATE TABLE business_approval_requests (
    request_id TEXT PRIMARY KEY,
    requester_instance TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    approval_kind TEXT NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'approved', 'rejected', 'cancelled', 'expired')
    ),
    revision BIGINT NOT NULL CHECK (revision >= 1),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    terminal_caller_instance TEXT,
    terminal_actor TEXT,
    evidence_ref TEXT,
    reason TEXT,
    terminal_at TIMESTAMPTZ,
    UNIQUE (requester_instance, idempotency_key),
    CHECK (expires_at > requested_at),
    CHECK (
        (
            status = 'pending'
            AND revision = 1
            AND terminal_caller_instance IS NULL
            AND terminal_actor IS NULL
            AND evidence_ref IS NULL
            AND reason IS NULL
            AND terminal_at IS NULL
        )
        OR (
            status IN ('approved', 'rejected')
            AND revision = 2
            AND terminal_caller_instance IS NOT NULL
            AND terminal_actor IS NOT NULL
            AND evidence_ref IS NOT NULL
            AND terminal_at IS NOT NULL
        )
        OR (
            status = 'cancelled'
            AND revision = 2
            AND terminal_caller_instance IS NOT NULL
            AND terminal_actor IS NOT NULL
            AND evidence_ref IS NULL
            AND terminal_at IS NOT NULL
        )
        OR (
            status = 'expired'
            AND revision = 2
            AND terminal_caller_instance IS NOT NULL
            AND terminal_actor IS NULL
            AND evidence_ref IS NULL
            AND reason IS NULL
            AND terminal_at IS NOT NULL
        )
    )
);

CREATE INDEX business_approval_due_lookup
    ON business_approval_requests(status, expires_at)
    WHERE status = 'pending';

CREATE INDEX business_approval_subject_lookup
    ON business_approval_requests(subject_kind, subject_id, requested_at DESC);
