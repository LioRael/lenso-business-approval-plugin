use time::OffsetDateTime;
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Rejected,
    Cancelled,
    Expired,
}

impl ApprovalStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }

    pub fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "rejected" => Ok(Self::Rejected),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            _ => Err(StorageError::InvalidStatus {
                status: value.to_owned(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RequestIntent {
    pub request_id: String,
    pub requester_instance: String,
    pub idempotency_key: String,
    pub requested_by: String,
    pub approval_kind: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub intent_digest: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub requested_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredApproval {
    pub request_id: String,
    pub requester_instance: String,
    pub idempotency_key: String,
    pub requested_by: String,
    pub approval_kind: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub intent_digest: Option<String>,
    pub status: ApprovalStatus,
    pub revision: i64,
    #[serde(with = "time::serde::rfc3339")]
    pub requested_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
    pub terminal_caller_instance: Option<String>,
    pub terminal_actor: Option<String>,
    pub evidence_ref: Option<String>,
    pub reason: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub terminal_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RequestOutcome {
    pub created: bool,
    pub approval: StoredApproval,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DomainFailure {
    IdempotencyConflict,
    RequestNotFound,
    AlreadyTerminal,
    NotRequester,
    NotDue,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Business Approval storage unavailable; a submitted transition may have committed")]
    Unavailable,
    #[error("stored status is invalid")]
    InvalidStatus { status: String },
    #[error("stored revision is invalid")]
    InvalidRevision,
    #[error("stored terminal evidence is inconsistent")]
    InvalidEvidence,
    #[error("idempotency result is inconsistent")]
    InconsistentIdempotency,
}
pub fn validate_evidence(approval: &StoredApproval) -> Result<(), StorageError> {
    let valid = match approval.status {
        ApprovalStatus::Pending => {
            approval.revision == 1
                && approval.terminal_caller_instance.is_none()
                && approval.terminal_actor.is_none()
                && approval.evidence_ref.is_none()
                && approval.reason.is_none()
                && approval.terminal_at.is_none()
        }
        ApprovalStatus::Approved | ApprovalStatus::Rejected => {
            approval.revision == 2
                && approval.terminal_caller_instance.is_some()
                && approval.terminal_actor.is_some()
                && approval.evidence_ref.is_some()
                && approval.terminal_at.is_some()
        }
        ApprovalStatus::Cancelled => {
            approval.revision == 2
                && approval.terminal_caller_instance.is_some()
                && approval.terminal_actor.is_some()
                && approval.evidence_ref.is_none()
                && approval.terminal_at.is_some()
        }
        ApprovalStatus::Expired => {
            approval.revision == 2
                && approval.terminal_caller_instance.is_some()
                && approval.terminal_actor.is_none()
                && approval.evidence_ref.is_none()
                && approval.reason.is_none()
                && approval.terminal_at.is_some()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(StorageError::InvalidEvidence)
    }
}

pub fn same_intent(approval: &StoredApproval, intent: &RequestIntent) -> bool {
    approval.request_id == intent.request_id
        && approval.requester_instance == intent.requester_instance
        && approval.idempotency_key == intent.idempotency_key
        && approval.requested_by == intent.requested_by
        && approval.approval_kind == intent.approval_kind
        && approval.subject_kind == intent.subject_kind
        && approval.subject_id == intent.subject_id
        && approval.intent_digest == intent.intent_digest
        && approval.expires_at == intent.expires_at
}
