use lenso_postgres_kit::OwnedPostgres;
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use thiserror::Error;

pub(crate) use lenso_business_approval_core::model::{
    ApprovalStatus, DomainFailure, RequestIntent, RequestOutcome, StoredApproval, same_intent,
};

#[derive(Debug, Error)]
pub(crate) enum StorageError {
    #[error("PostgreSQL operation `{operation}` failed")]
    Database {
        operation: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("stored Business Approval status `{status}` is invalid")]
    InvalidStatus { status: String },
    #[error("stored Business Approval revision is invalid")]
    InvalidRevision,
    #[error("stored Business Approval terminal evidence is inconsistent")]
    InvalidEvidence,
    #[error("idempotency conflict could not be resolved to an existing request")]
    InconsistentIdempotency,
}

pub(crate) async fn request(
    postgres: &OwnedPostgres,
    intent: &RequestIntent,
) -> Result<Result<RequestOutcome, DomainFailure>, StorageError> {
    let mut transaction = postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database("begin request", source))?;
    let inserted = sqlx::query(
        "INSERT INTO business_approval_requests(request_id,requester_instance,idempotency_key,requested_by,approval_kind,subject_kind,subject_id,status,revision,requested_at,expires_at,intent_digest) VALUES($1,$2,$3,$4,$5,$6,$7,'pending',1,$8,$9,$10) ON CONFLICT DO NOTHING RETURNING request_id,requester_instance,idempotency_key,requested_by,approval_kind,subject_kind,subject_id,status,revision,requested_at,expires_at,terminal_caller_instance,terminal_actor,evidence_ref,reason,terminal_at,intent_digest",
    )
    .bind(&intent.request_id)
    .bind(&intent.requester_instance)
    .bind(&intent.idempotency_key)
    .bind(&intent.requested_by)
    .bind(&intent.approval_kind)
    .bind(&intent.subject_kind)
    .bind(&intent.subject_id)
    .bind(intent.requested_at)
    .bind(intent.expires_at)
    .bind(&intent.intent_digest)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|source| database("create approval request", source))?;

    if let Some(row) = inserted {
        let approval = decode_approval(&row)?;
        commit(transaction, "commit request creation").await?;
        return Ok(Ok(RequestOutcome {
            created: true,
            approval,
        }));
    }

    let rows = sqlx::query(
        "SELECT request_id,requester_instance,idempotency_key,requested_by,approval_kind,subject_kind,subject_id,status,revision,requested_at,expires_at,terminal_caller_instance,terminal_actor,evidence_ref,reason,terminal_at,intent_digest FROM business_approval_requests WHERE (requester_instance=$1 AND idempotency_key=$2) OR request_id=$3 FOR UPDATE",
    )
    .bind(&intent.requester_instance)
    .bind(&intent.idempotency_key)
    .bind(&intent.request_id)
    .fetch_all(&mut *transaction)
    .await
    .map_err(|source| database("resolve idempotent request", source))?;

    if rows.len() != 1 {
        if rows.is_empty() {
            return Err(StorageError::InconsistentIdempotency);
        }
        return Ok(Err(DomainFailure::IdempotencyConflict));
    }
    let approval = decode_approval(&rows[0])?;
    if !same_intent(&approval, intent) {
        return Ok(Err(DomainFailure::IdempotencyConflict));
    }
    commit(transaction, "commit idempotent request").await?;
    Ok(Ok(RequestOutcome {
        created: false,
        approval,
    }))
}

pub(crate) async fn read(
    postgres: &OwnedPostgres,
    request_id: &str,
    requester_constraint: Option<&str>,
) -> Result<Option<StoredApproval>, StorageError> {
    let row = sqlx::query(
        "SELECT request_id,requester_instance,idempotency_key,requested_by,approval_kind,subject_kind,subject_id,status,revision,requested_at,expires_at,terminal_caller_instance,terminal_actor,evidence_ref,reason,terminal_at,intent_digest FROM business_approval_requests WHERE request_id=$1 AND ($2::text IS NULL OR requester_instance=$2)",
    )
    .bind(request_id)
    .bind(requester_constraint)
    .fetch_optional(postgres.pool())
    .await
    .map_err(|source| database("read approval request", source))?;
    row.as_ref().map(decode_approval).transpose()
}

pub(crate) async fn decide(
    postgres: &OwnedPostgres,
    request_id: &str,
    decision: ApprovalStatus,
    caller: &str,
    actor: &str,
    evidence_ref: &str,
    reason: Option<&str>,
) -> Result<Result<StoredApproval, DomainFailure>, StorageError> {
    debug_assert!(matches!(
        decision,
        ApprovalStatus::Approved | ApprovalStatus::Rejected
    ));
    let mut transaction = begin_transition(postgres, "begin decision").await?;
    let Some(current) = lock_request(&mut transaction, request_id).await? else {
        return Ok(Err(DomainFailure::RequestNotFound));
    };
    if current.status != ApprovalStatus::Pending {
        return Ok(Err(DomainFailure::AlreadyTerminal));
    }
    let approval = sqlx::query(
        "UPDATE business_approval_requests SET status=$2,revision=revision+1,terminal_caller_instance=$3,terminal_actor=$4,evidence_ref=$5,reason=$6,terminal_at=transaction_timestamp() WHERE request_id=$1 RETURNING request_id,requester_instance,idempotency_key,requested_by,approval_kind,subject_kind,subject_id,status,revision,requested_at,expires_at,terminal_caller_instance,terminal_actor,evidence_ref,reason,terminal_at,intent_digest",
    )
    .bind(request_id)
    .bind(decision.as_str())
    .bind(caller)
    .bind(actor)
    .bind(evidence_ref)
    .bind(reason)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("record decision", source))
    .and_then(|row| decode_approval(&row))?;
    commit(transaction, "commit decision").await?;
    Ok(Ok(approval))
}

pub(crate) async fn cancel(
    postgres: &OwnedPostgres,
    request_id: &str,
    caller: &str,
    actor: &str,
    reason: Option<&str>,
) -> Result<Result<StoredApproval, DomainFailure>, StorageError> {
    let mut transaction = begin_transition(postgres, "begin cancellation").await?;
    let Some(current) = lock_request(&mut transaction, request_id).await? else {
        return Ok(Err(DomainFailure::RequestNotFound));
    };
    if current.requester_instance != caller {
        return Ok(Err(DomainFailure::NotRequester));
    }
    if current.status != ApprovalStatus::Pending {
        return Ok(Err(DomainFailure::AlreadyTerminal));
    }
    let approval = sqlx::query(
        "UPDATE business_approval_requests SET status='cancelled',revision=revision+1,terminal_caller_instance=$2,terminal_actor=$3,evidence_ref=NULL,reason=$4,terminal_at=transaction_timestamp() WHERE request_id=$1 RETURNING request_id,requester_instance,idempotency_key,requested_by,approval_kind,subject_kind,subject_id,status,revision,requested_at,expires_at,terminal_caller_instance,terminal_actor,evidence_ref,reason,terminal_at,intent_digest",
    )
    .bind(request_id)
    .bind(caller)
    .bind(actor)
    .bind(reason)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("cancel approval", source))
    .and_then(|row| decode_approval(&row))?;
    commit(transaction, "commit cancellation").await?;
    Ok(Ok(approval))
}

pub(crate) async fn expire(
    postgres: &OwnedPostgres,
    request_id: &str,
    caller: &str,
) -> Result<Result<StoredApproval, DomainFailure>, StorageError> {
    let mut transaction = begin_transition(postgres, "begin expiration").await?;
    let Some(current) = lock_request(&mut transaction, request_id).await? else {
        return Ok(Err(DomainFailure::RequestNotFound));
    };
    if current.status != ApprovalStatus::Pending {
        return Ok(Err(DomainFailure::AlreadyTerminal));
    }
    let due: bool = sqlx::query_scalar(
        "SELECT expires_at <= transaction_timestamp() FROM business_approval_requests WHERE request_id=$1",
    )
    .bind(request_id)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("check expiration deadline", source))?;
    if !due {
        return Ok(Err(DomainFailure::NotDue));
    }
    let approval = sqlx::query(
        "UPDATE business_approval_requests SET status='expired',revision=revision+1,terminal_caller_instance=$2,terminal_actor=NULL,evidence_ref=NULL,reason=NULL,terminal_at=transaction_timestamp() WHERE request_id=$1 RETURNING request_id,requester_instance,idempotency_key,requested_by,approval_kind,subject_kind,subject_id,status,revision,requested_at,expires_at,terminal_caller_instance,terminal_actor,evidence_ref,reason,terminal_at,intent_digest",
    )
    .bind(request_id)
    .bind(caller)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|source| database("expire approval", source))
    .and_then(|row| decode_approval(&row))?;
    commit(transaction, "commit expiration").await?;
    Ok(Ok(approval))
}

async fn begin_transition<'a>(
    postgres: &'a OwnedPostgres,
    operation: &'static str,
) -> Result<Transaction<'a, Postgres>, StorageError> {
    postgres
        .pool()
        .begin()
        .await
        .map_err(|source| database(operation, source))
}

async fn lock_request(
    transaction: &mut Transaction<'_, Postgres>,
    request_id: &str,
) -> Result<Option<StoredApproval>, StorageError> {
    let row = sqlx::query(
        "SELECT request_id,requester_instance,idempotency_key,requested_by,approval_kind,subject_kind,subject_id,status,revision,requested_at,expires_at,terminal_caller_instance,terminal_actor,evidence_ref,reason,terminal_at,intent_digest FROM business_approval_requests WHERE request_id=$1 FOR UPDATE",
    )
    .bind(request_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| database("lock approval request", source))?;
    row.as_ref().map(decode_approval).transpose()
}

fn decode_approval(row: &PgRow) -> Result<StoredApproval, StorageError> {
    let status_text: String = value(row, "status", "decode status")?;
    let status = ApprovalStatus::parse(&status_text).map_err(|_| StorageError::InvalidStatus {
        status: status_text.clone(),
    })?;
    let revision: i64 = value(row, "revision", "decode revision")?;
    if revision < 1 {
        return Err(StorageError::InvalidRevision);
    }
    let approval = StoredApproval {
        request_id: value(row, "request_id", "decode request ID")?,
        requester_instance: value(row, "requester_instance", "decode requester Instance")?,
        idempotency_key: value(row, "idempotency_key", "decode idempotency key")?,
        requested_by: value(row, "requested_by", "decode requester")?,
        approval_kind: value(row, "approval_kind", "decode approval kind")?,
        subject_kind: value(row, "subject_kind", "decode subject kind")?,
        subject_id: value(row, "subject_id", "decode subject ID")?,
        intent_digest: value(row, "intent_digest", "decode intent digest")?,
        status,
        revision,
        requested_at: value(row, "requested_at", "decode request timestamp")?,
        expires_at: value(row, "expires_at", "decode expiration timestamp")?,
        terminal_caller_instance: value(row, "terminal_caller_instance", "decode terminal caller")?,
        terminal_actor: value(row, "terminal_actor", "decode terminal actor")?,
        evidence_ref: value(row, "evidence_ref", "decode evidence reference")?,
        reason: value(row, "reason", "decode reason")?,
        terminal_at: value(row, "terminal_at", "decode terminal timestamp")?,
    };
    validate_evidence(&approval)?;
    Ok(approval)
}

fn validate_evidence(approval: &StoredApproval) -> Result<(), StorageError> {
    lenso_business_approval_core::model::validate_evidence(approval)
        .map_err(|_| StorageError::InvalidEvidence)
}

fn value<T>(row: &PgRow, column: &'static str, operation: &'static str) -> Result<T, StorageError>
where
    for<'row> T: sqlx::Decode<'row, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get(column)
        .map_err(|source| database(operation, source))
}

async fn commit(
    transaction: Transaction<'_, Postgres>,
    operation: &'static str,
) -> Result<(), StorageError> {
    transaction
        .commit()
        .await
        .map_err(|source| database(operation, source))
}

fn database(operation: &'static str, source: sqlx::Error) -> StorageError {
    StorageError::Database { operation, source }
}
