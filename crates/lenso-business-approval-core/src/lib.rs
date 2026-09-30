//! Owner-private request validation, authority and state projection shared by PG and D1.
pub mod model;
use lenso::prelude::*;
use lenso_capability_business_approval::{
    CancelError, CancelRequest, CancelResponse, CancelResponseStatus, DecideError, DecideRequest,
    DecideRequestDecision, DecideResponse, DecideResponseStatus, ExpireError, ExpireRequest,
    ExpireResponse, ExpireResponseStatus, ReadError, ReadRequest, ReadResponse, ReadResponseStatus,
    ReadResponseSubject, RequestError, RequestRequest, RequestResponse, RequestResponseStatus,
};
use model::{
    ApprovalStatus, DomainFailure, RequestIntent, RequestOutcome, StorageError, StoredApproval,
};
use std::collections::BTreeSet;
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
const MAX_CALLERS: usize = 64;
const MAX_REQUEST_ID_BYTES: usize = 128;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const MAX_KIND_BYTES: usize = 128;
const MAX_REFERENCE_BYTES: usize = 512;
const MAX_REASON_BYTES: usize = 1_000;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApprovalPolicy {
    pub requester_instances: Vec<String>,
    pub decider_instances: Vec<String>,
    pub expiration_executor_instances: Vec<String>,
}
impl ApprovalPolicy {
    pub fn validate(&self) -> Result<(), RuntimeFailure> {
        for values in [
            &self.requester_instances,
            &self.decider_instances,
            &self.expiration_executor_instances,
        ] {
            if validate_callers(values).is_err() {
                return Err(RuntimeFailure::InvalidResolvedPlan {
                    detail: "invalid Business Approval exact caller policy".into(),
                });
            }
        }
        Ok(())
    }
    fn can_request(&self, caller: &str) -> bool {
        contains_exact(&self.requester_instances, caller)
    }

    fn can_decide(&self, caller: &str) -> bool {
        contains_exact(&self.decider_instances, caller)
    }

    fn can_expire(&self, caller: &str) -> bool {
        contains_exact(&self.expiration_executor_instances, caller)
    }

    fn can_read(&self, caller: &str) -> bool {
        self.can_request(caller) || self.can_decide(caller) || self.can_expire(caller)
    }

    fn can_read_any_request(&self, caller: &str) -> bool {
        self.can_decide(caller) || self.can_expire(caller)
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CallerListError {
    #[error("the list must contain between 1 and 64 Instance keys")]
    EmptyOrTooLarge,
    #[error("an Instance key is invalid")]
    InvalidInstance,
    #[error("Instance keys must not be duplicated")]
    DuplicateInstance,
}

#[allow(async_fn_in_trait)]
pub trait ApprovalStore: Clone {
    fn now(&self) -> Result<OffsetDateTime, StorageError>;
    async fn request(
        &self,
        intent: &RequestIntent,
    ) -> Result<Result<RequestOutcome, DomainFailure>, StorageError>;
    async fn read(
        &self,
        id: &str,
        requester: Option<&str>,
    ) -> Result<Option<StoredApproval>, StorageError>;
    async fn decide(
        &self,
        id: &str,
        status: ApprovalStatus,
        caller: &str,
        actor: &str,
        evidence: &str,
        reason: Option<&str>,
    ) -> Result<Result<StoredApproval, DomainFailure>, StorageError>;
    async fn cancel(
        &self,
        id: &str,
        caller: &str,
        actor: &str,
        reason: Option<&str>,
    ) -> Result<Result<StoredApproval, DomainFailure>, StorageError>;
    async fn expire(
        &self,
        id: &str,
        caller: &str,
    ) -> Result<Result<StoredApproval, DomainFailure>, StorageError>;
}
#[derive(Clone, Debug)]
pub struct ApprovalService<S> {
    pub config: ApprovalPolicy,
    pub store: S,
}
impl<S: ApprovalStore> ApprovalService<S> {
    pub async fn request(
        &self,
        context: Ctx,
        request: RequestRequest,
    ) -> PluginResult<RequestResponse, RequestError> {
        let caller = self
            .authorized_caller(&context, ApprovalPolicy::can_request)
            .ok_or_else(|| PluginError::domain(RequestError::Forbidden))?;
        let requested_at = normalize_timestamp(self.store.now().map_err(storage_runtime)?)?;
        let expires_at = OffsetDateTime::parse(&request.expires_at, &Rfc3339)
            .map_err(|_| PluginError::domain(RequestError::InvalidRequest))?;
        let expires_at = normalize_timestamp(expires_at)?;
        if !valid_request_id(&request.request_id)
            || !valid_idempotency_key(&request.idempotency_key)
            || !valid_reference(&request.requested_by)
            || !valid_kind(&request.approval_kind)
            || !valid_kind(&request.subject.kind)
            || !valid_reference(&request.subject.id)
            || request
                .intent_digest
                .as_deref()
                .is_some_and(|digest| !valid_intent_digest(digest))
            || (request.subject.kind == "management-operation" && request.intent_digest.is_none())
            || expires_at <= requested_at
        {
            return Err(PluginError::domain(RequestError::InvalidRequest));
        }
        let outcome = self
            .store
            .request(&RequestIntent {
                request_id: request.request_id,
                requester_instance: caller,
                idempotency_key: request.idempotency_key,
                requested_by: request.requested_by,
                approval_kind: request.approval_kind,
                subject_kind: request.subject.kind,
                subject_id: request.subject.id,
                intent_digest: request.intent_digest,
                requested_at,
                expires_at,
            })
            .await
            .map_err(storage_runtime)?
            .map_err(|failure| PluginError::domain(map_request_failure(failure)))?;
        Ok(RequestResponse {
            created: outcome.created,
            request_id: outcome.approval.request_id,
            status: request_status(outcome.approval.status),
            revision: revision_string(outcome.approval.revision)?,
        })
    }

    pub async fn decide(
        &self,
        context: Ctx,
        request: DecideRequest,
    ) -> PluginResult<DecideResponse, DecideError> {
        let caller = self
            .authorized_caller(&context, ApprovalPolicy::can_decide)
            .ok_or_else(|| PluginError::domain(DecideError::Forbidden))?;
        if lenso_auth_sdk::delegation::denies_human_context(&context) {
            return Err(PluginError::domain(DecideError::Forbidden));
        }
        if !valid_request_id(&request.request_id)
            || !valid_reference(&request.decided_by)
            || !valid_reference(&request.evidence_ref)
            || !valid_reason(request.reason.as_deref())
        {
            return Err(PluginError::domain(DecideError::InvalidRequest));
        }
        let decision = match request.decision {
            DecideRequestDecision::Approved => ApprovalStatus::Approved,
            DecideRequestDecision::Rejected => ApprovalStatus::Rejected,
        };
        let approval = self
            .store
            .decide(
                &request.request_id,
                decision,
                &caller,
                &request.decided_by,
                &request.evidence_ref,
                request.reason.as_deref(),
            )
            .await
            .map_err(storage_runtime)?
            .map_err(|failure| PluginError::domain(map_decide_failure(failure)))?;
        decide_response(approval)
    }

    pub async fn cancel(
        &self,
        context: Ctx,
        request: CancelRequest,
    ) -> PluginResult<CancelResponse, CancelError> {
        let caller = self
            .authorized_caller(&context, ApprovalPolicy::can_request)
            .ok_or_else(|| PluginError::domain(CancelError::Forbidden))?;
        if !valid_request_id(&request.request_id)
            || !valid_reference(&request.cancelled_by)
            || !valid_reason(request.reason.as_deref())
        {
            return Err(PluginError::domain(CancelError::InvalidRequest));
        }
        let approval = self
            .store
            .cancel(
                &request.request_id,
                &caller,
                &request.cancelled_by,
                request.reason.as_deref(),
            )
            .await
            .map_err(storage_runtime)?
            .map_err(|failure| PluginError::domain(map_cancel_failure(failure)))?;
        cancel_response(approval)
    }

    pub async fn read(
        &self,
        context: Ctx,
        request: ReadRequest,
    ) -> PluginResult<ReadResponse, ReadError> {
        let caller = self
            .authorized_caller(&context, ApprovalPolicy::can_read)
            .ok_or_else(|| PluginError::domain(ReadError::Forbidden))?;
        if !valid_request_id(&request.request_id) {
            return Err(PluginError::domain(ReadError::InvalidRequest));
        }
        let requester_constraint =
            (!self.config.can_read_any_request(&caller)).then_some(caller.as_str());
        let approval = self
            .store
            .read(&request.request_id, requester_constraint)
            .await
            .map_err(storage_runtime)?
            .ok_or_else(|| PluginError::domain(ReadError::RequestNotFound))?;
        read_response(approval)
    }

    pub async fn expire(
        &self,
        context: Ctx,
        request: ExpireRequest,
    ) -> PluginResult<ExpireResponse, ExpireError> {
        let caller = self
            .authorized_caller(&context, ApprovalPolicy::can_expire)
            .ok_or_else(|| PluginError::domain(ExpireError::Forbidden))?;
        if !valid_request_id(&request.request_id) {
            return Err(PluginError::domain(ExpireError::InvalidRequest));
        }
        let approval = self
            .store
            .expire(&request.request_id, &caller)
            .await
            .map_err(storage_runtime)?
            .map_err(|failure| PluginError::domain(map_expire_failure(failure)))?;
        expire_response(approval)
    }

    fn authorized_caller(
        &self,
        context: &Ctx,
        predicate: fn(&ApprovalPolicy, &str) -> bool,
    ) -> Option<String> {
        context
            .caller_instance()
            .filter(|caller| predicate(&self.config, caller))
            .map(ToOwned::to_owned)
    }
}
fn decide_response(approval: StoredApproval) -> PluginResult<DecideResponse, DecideError> {
    Ok(DecideResponse {
        request_id: approval.request_id,
        status: match approval.status {
            ApprovalStatus::Approved => DecideResponseStatus::Approved,
            ApprovalStatus::Rejected => DecideResponseStatus::Rejected,
            _ => return Err(invalid_terminal()),
        },
        revision: revision_string(approval.revision)?,
        terminal_caller_instance: required_terminal(approval.terminal_caller_instance)?,
        terminal_actor: approval.terminal_actor,
        evidence_ref: approval.evidence_ref,
        reason: approval.reason,
        terminal_at: format_required_timestamp(approval.terminal_at)?,
    })
}

fn cancel_response(approval: StoredApproval) -> PluginResult<CancelResponse, CancelError> {
    if approval.status != ApprovalStatus::Cancelled {
        return Err(invalid_terminal());
    }
    Ok(CancelResponse {
        request_id: approval.request_id,
        status: CancelResponseStatus::Cancelled,
        revision: revision_string(approval.revision)?,
        terminal_caller_instance: required_terminal(approval.terminal_caller_instance)?,
        terminal_actor: approval.terminal_actor,
        evidence_ref: approval.evidence_ref,
        reason: approval.reason,
        terminal_at: format_required_timestamp(approval.terminal_at)?,
    })
}

fn expire_response(approval: StoredApproval) -> PluginResult<ExpireResponse, ExpireError> {
    if approval.status != ApprovalStatus::Expired {
        return Err(invalid_terminal());
    }
    Ok(ExpireResponse {
        request_id: approval.request_id,
        status: ExpireResponseStatus::Expired,
        revision: revision_string(approval.revision)?,
        terminal_caller_instance: required_terminal(approval.terminal_caller_instance)?,
        terminal_actor: approval.terminal_actor,
        evidence_ref: approval.evidence_ref,
        reason: approval.reason,
        terminal_at: format_required_timestamp(approval.terminal_at)?,
    })
}

fn read_response(approval: StoredApproval) -> PluginResult<ReadResponse, ReadError> {
    Ok(ReadResponse {
        request_id: approval.request_id,
        idempotency_key: approval.idempotency_key,
        approval_kind: approval.approval_kind,
        subject: ReadResponseSubject {
            kind: approval.subject_kind,
            id: approval.subject_id,
        },
        intent_digest: approval.intent_digest,
        requester_instance: approval.requester_instance,
        requested_by: approval.requested_by,
        status: match approval.status {
            ApprovalStatus::Pending => ReadResponseStatus::Pending,
            ApprovalStatus::Approved => ReadResponseStatus::Approved,
            ApprovalStatus::Rejected => ReadResponseStatus::Rejected,
            ApprovalStatus::Cancelled => ReadResponseStatus::Cancelled,
            ApprovalStatus::Expired => ReadResponseStatus::Expired,
        },
        revision: revision_string(approval.revision)?,
        requested_at: format_timestamp(approval.requested_at)?,
        expires_at: format_timestamp(approval.expires_at)?,
        terminal_caller_instance: approval.terminal_caller_instance,
        terminal_actor: approval.terminal_actor,
        evidence_ref: approval.evidence_ref,
        reason: approval.reason,
        terminal_at: approval.terminal_at.map(format_timestamp).transpose()?,
    })
}

fn valid_intent_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn request_status(status: ApprovalStatus) -> RequestResponseStatus {
    match status {
        ApprovalStatus::Pending => RequestResponseStatus::Pending,
        ApprovalStatus::Approved => RequestResponseStatus::Approved,
        ApprovalStatus::Rejected => RequestResponseStatus::Rejected,
        ApprovalStatus::Cancelled => RequestResponseStatus::Cancelled,
        ApprovalStatus::Expired => RequestResponseStatus::Expired,
    }
}

fn required_terminal<E>(value: Option<String>) -> Result<String, PluginError<E>> {
    value.ok_or_else(invalid_terminal)
}

fn format_required_timestamp<E>(value: Option<OffsetDateTime>) -> Result<String, PluginError<E>> {
    value
        .ok_or_else(invalid_terminal)
        .and_then(format_timestamp)
}

fn normalize_timestamp<E>(value: OffsetDateTime) -> Result<OffsetDateTime, PluginError<E>> {
    let nanos = value.unix_timestamp_nanos();
    let micros = nanos - nanos.rem_euclid(1_000);
    OffsetDateTime::from_unix_timestamp_nanos(micros)
        .map_err(|_| invalid_request_runtime("timestamp is outside PostgreSQL range"))
}

fn format_timestamp<E>(value: OffsetDateTime) -> Result<String, PluginError<E>> {
    value
        .format(&Rfc3339)
        .map_err(|_| invalid_request_runtime("stored timestamp cannot be formatted"))
}

fn revision_string<E>(revision: i64) -> Result<String, PluginError<E>> {
    if revision < 1 {
        Err(invalid_request_runtime("stored revision is invalid"))
    } else {
        Ok(revision.to_string())
    }
}

fn invalid_terminal<E>() -> PluginError<E> {
    invalid_request_runtime("stored terminal evidence is inconsistent")
}

fn invalid_request_runtime<E>(detail: &str) -> PluginError<E> {
    PluginError::runtime(RuntimeFailure::Internal {
        detail: format!("Business Approval: {detail}"),
    })
}

#[allow(clippy::needless_pass_by_value)]
fn storage_runtime<E>(error: StorageError) -> PluginError<E> {
    PluginError::runtime(RuntimeFailure::PluginFailure {
        detail: error.to_string(),
    })
}

fn map_request_failure(failure: DomainFailure) -> RequestError {
    match failure {
        DomainFailure::IdempotencyConflict => RequestError::IdempotencyConflict,
        _ => RequestError::InvalidRequest,
    }
}

fn map_decide_failure(failure: DomainFailure) -> DecideError {
    match failure {
        DomainFailure::RequestNotFound => DecideError::RequestNotFound,
        DomainFailure::AlreadyTerminal => DecideError::AlreadyTerminal,
        _ => DecideError::InvalidRequest,
    }
}

fn map_cancel_failure(failure: DomainFailure) -> CancelError {
    match failure {
        DomainFailure::RequestNotFound => CancelError::RequestNotFound,
        DomainFailure::AlreadyTerminal => CancelError::AlreadyTerminal,
        DomainFailure::NotRequester => CancelError::NotRequester,
        _ => CancelError::InvalidRequest,
    }
}

fn map_expire_failure(failure: DomainFailure) -> ExpireError {
    match failure {
        DomainFailure::RequestNotFound => ExpireError::RequestNotFound,
        DomainFailure::AlreadyTerminal => ExpireError::AlreadyTerminal,
        DomainFailure::NotDue => ExpireError::NotDue,
        _ => ExpireError::InvalidRequest,
    }
}

pub fn validate_callers(callers: &[String]) -> Result<(), CallerListError> {
    if callers.is_empty() || callers.len() > MAX_CALLERS {
        return Err(CallerListError::EmptyOrTooLarge);
    }
    if callers.iter().any(|caller| !valid_instance(caller)) {
        return Err(CallerListError::InvalidInstance);
    }
    if callers.iter().collect::<BTreeSet<_>>().len() != callers.len() {
        return Err(CallerListError::DuplicateInstance);
    }
    Ok(())
}

fn contains_exact(callers: &[String], caller: &str) -> bool {
    callers.iter().any(|allowed| allowed == caller)
}

pub fn valid_request_id(value: &str) -> bool {
    valid_identifier(value, MAX_REQUEST_ID_BYTES)
}

fn valid_idempotency_key(value: &str) -> bool {
    valid_identifier(value, MAX_IDEMPOTENCY_KEY_BYTES)
}

fn valid_instance(value: &str) -> bool {
    value.len() <= 256
        && (1..=2).contains(&value.split('/').count())
        && value
            .split('/')
            .all(|segment| valid_identifier(segment, 256))
}

pub fn valid_kind(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_KIND_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn valid_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REFERENCE_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

fn valid_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

pub fn valid_reason(reason: Option<&str>) -> bool {
    reason.is_none_or(|value| {
        let trimmed = value.trim();
        !trimmed.is_empty()
            && value.len() <= MAX_REASON_BYTES
            && !value.chars().any(char::is_control)
    })
}
