//! PostgreSQL-backed independent Business Approval Plugin.

mod operator;
#[cfg(all(test, feature = "postgres-acceptance"))]
mod postgres_tests;
mod schema;
mod storage;

use std::{cell::RefCell, collections::BTreeSet, fmt, rc::Rc, time::Duration};

use lenso::prelude::*;
use lenso_capability_business_approval as approval;
use lenso_capability_business_approval::{
    CancelError, CancelRequest, CancelResponse, CancelResponseStatus, DecideError, DecideRequest,
    DecideRequestDecision, DecideResponse, DecideResponseStatus, ExpireError, ExpireRequest,
    ExpireResponse, ExpireResponseStatus, ReadError, ReadRequest, ReadResponse, ReadResponseStatus,
    ReadResponseSubject, RequestError, RequestRequest, RequestResponse, RequestResponseStatus,
};
use lenso_capability_secrets as secrets;
use lenso_capability_secrets::{ResolveRequest, SecretsClient, SecretsInvocationError};
use lenso_kernel::{PluginDependencies, RuntimeFailure};
use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use zeroize::Zeroizing;

use crate::storage::{ApprovalStatus, DomainFailure, RequestIntent, StoredApproval};

pub use operator::{BusinessApprovalOperator, BusinessApprovalOperatorError};

const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CALLERS: usize = 64;
const MAX_REQUEST_ID_BYTES: usize = 128;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;
const MAX_KIND_BYTES: usize = 128;
const MAX_REFERENCE_BYTES: usize = 512;
const MAX_REASON_BYTES: usize = 1_000;

/// Immutable configuration for one `PostgreSQL` Business Approval Instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BusinessApprovalConfig {
    schema: String,
    database_url_secret: String,
    requester_instances: Vec<String>,
    decider_instances: Vec<String>,
    expiration_executor_instances: Vec<String>,
}

impl BusinessApprovalConfig {
    /// Creates and validates one Business Approval Instance configuration.
    pub fn new(
        schema: impl Into<String>,
        database_url_secret: impl Into<String>,
        requester_instances: Vec<String>,
        decider_instances: Vec<String>,
        expiration_executor_instances: Vec<String>,
    ) -> Result<Self, BusinessApprovalConfigError> {
        let config = Self {
            schema: schema.into(),
            database_url_secret: database_url_secret.into(),
            requester_instances,
            decider_instances,
            expiration_executor_instances,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), BusinessApprovalConfigError> {
        schema::schema_plan(self.schema.clone())
            .map_err(|_| BusinessApprovalConfigError::InvalidSchema)?;
        if !valid_secret_reference(&self.database_url_secret) {
            return Err(BusinessApprovalConfigError::InvalidSecretReference);
        }
        validate_callers(&self.requester_instances)
            .map_err(BusinessApprovalConfigError::InvalidRequesters)?;
        validate_callers(&self.decider_instances)
            .map_err(BusinessApprovalConfigError::InvalidDeciders)?;
        validate_callers(&self.expiration_executor_instances)
            .map_err(BusinessApprovalConfigError::InvalidExpirationExecutors)?;
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

/// Invalid immutable Business Approval configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BusinessApprovalConfigError {
    #[error("invalid owned PostgreSQL schema")]
    InvalidSchema,
    #[error("invalid database URL secret reference")]
    InvalidSecretReference,
    #[error("invalid requester_instances: {0}")]
    InvalidRequesters(CallerListError),
    #[error("invalid decider_instances: {0}")]
    InvalidDeciders(CallerListError),
    #[error("invalid expiration_executor_instances: {0}")]
    InvalidExpirationExecutors(CallerListError),
}

/// Invalid exact caller allowlist.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CallerListError {
    #[error("the list must contain between 1 and 64 Instance keys")]
    EmptyOrTooLarge,
    #[error("an Instance key is invalid")]
    InvalidInstance,
    #[error("Instance keys must not be duplicated")]
    DuplicateInstance,
}

fn validate_config(config: &BusinessApprovalConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("Business Approval configuration is invalid: {error}"),
        })
}

#[derive(Clone, Debug)]
struct PreparedBusinessApproval {
    postgres: OwnedPostgres,
}

#[lenso::plugin(
    lifecycle,
    configuration_schema = "config.schema.json",
    validate = validate_config
)]
#[derive(Clone)]
struct PostgresBusinessApprovalPlugin {
    #[config]
    config: BusinessApprovalConfig,
    secrets: Port<secrets::SecretsClient>,
    prepared: Rc<RefCell<Option<PreparedBusinessApproval>>>,
}

impl fmt::Debug for PostgresBusinessApprovalPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresBusinessApprovalPlugin")
            .field("prepared", &self.prepared.borrow().is_some())
            .field("schema", &self.config.schema)
            .field("requester_count", &self.config.requester_instances.len())
            .field("decider_count", &self.config.decider_instances.len())
            .field(
                "expiration_executor_count",
                &self.config.expiration_executor_instances.len(),
            )
            .finish_non_exhaustive()
    }
}

#[lenso::provides(approval::BusinessApproval)]
impl PostgresBusinessApprovalPlugin {}

impl PostgresBusinessApprovalPlugin {
    async fn request(
        &self,
        context: Ctx,
        request: RequestRequest,
    ) -> PluginResult<RequestResponse, RequestError> {
        let caller = self
            .authorized_caller(&context, BusinessApprovalConfig::can_request)
            .ok_or_else(|| PluginError::domain(RequestError::Forbidden))?;
        let requested_at = normalize_timestamp(OffsetDateTime::now_utc())?;
        let expires_at = OffsetDateTime::parse(&request.expires_at, &Rfc3339)
            .map_err(|_| PluginError::domain(RequestError::InvalidRequest))?;
        let expires_at = normalize_timestamp(expires_at)?;
        if !valid_request_id(&request.request_id)
            || !valid_idempotency_key(&request.idempotency_key)
            || !valid_reference(&request.requested_by)
            || !valid_kind(&request.approval_kind)
            || !valid_kind(&request.subject.kind)
            || !valid_reference(&request.subject.id)
            || expires_at <= requested_at
        {
            return Err(PluginError::domain(RequestError::InvalidRequest));
        }
        let outcome = storage::request(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &RequestIntent {
                request_id: request.request_id,
                requester_instance: caller,
                idempotency_key: request.idempotency_key,
                requested_by: request.requested_by,
                approval_kind: request.approval_kind,
                subject_kind: request.subject.kind,
                subject_id: request.subject.id,
                requested_at,
                expires_at,
            },
        )
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

    async fn decide(
        &self,
        context: Ctx,
        request: DecideRequest,
    ) -> PluginResult<DecideResponse, DecideError> {
        let caller = self
            .authorized_caller(&context, BusinessApprovalConfig::can_decide)
            .ok_or_else(|| PluginError::domain(DecideError::Forbidden))?;
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
        let approval = storage::decide(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
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

    async fn cancel(
        &self,
        context: Ctx,
        request: CancelRequest,
    ) -> PluginResult<CancelResponse, CancelError> {
        let caller = self
            .authorized_caller(&context, BusinessApprovalConfig::can_request)
            .ok_or_else(|| PluginError::domain(CancelError::Forbidden))?;
        if !valid_request_id(&request.request_id)
            || !valid_reference(&request.cancelled_by)
            || !valid_reason(request.reason.as_deref())
        {
            return Err(PluginError::domain(CancelError::InvalidRequest));
        }
        let approval = storage::cancel(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
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

    async fn read(
        &self,
        context: Ctx,
        request: ReadRequest,
    ) -> PluginResult<ReadResponse, ReadError> {
        let caller = self
            .authorized_caller(&context, BusinessApprovalConfig::can_read)
            .ok_or_else(|| PluginError::domain(ReadError::Forbidden))?;
        if !valid_request_id(&request.request_id) {
            return Err(PluginError::domain(ReadError::InvalidRequest));
        }
        let requester_constraint =
            (!self.config.can_read_any_request(&caller)).then_some(caller.as_str());
        let approval = storage::read(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.request_id,
            requester_constraint,
        )
        .await
        .map_err(storage_runtime)?
        .ok_or_else(|| PluginError::domain(ReadError::RequestNotFound))?;
        read_response(approval)
    }

    async fn expire(
        &self,
        context: Ctx,
        request: ExpireRequest,
    ) -> PluginResult<ExpireResponse, ExpireError> {
        let caller = self
            .authorized_caller(&context, BusinessApprovalConfig::can_expire)
            .ok_or_else(|| PluginError::domain(ExpireError::Forbidden))?;
        if !valid_request_id(&request.request_id) {
            return Err(PluginError::domain(ExpireError::InvalidRequest));
        }
        let approval = storage::expire(
            &self.prepared().map_err(PluginError::runtime)?.postgres,
            &request.request_id,
            &caller,
        )
        .await
        .map_err(storage_runtime)?
        .map_err(|failure| PluginError::domain(map_expire_failure(failure)))?;
        expire_response(approval)
    }

    fn authorized_caller(
        &self,
        context: &Ctx,
        predicate: fn(&BusinessApprovalConfig, &str) -> bool,
    ) -> Option<String> {
        context
            .caller_instance()
            .filter(|caller| predicate(&self.config, caller))
            .map(ToOwned::to_owned)
    }

    fn prepared(&self) -> Result<PreparedBusinessApproval, RuntimeFailure> {
        self.prepared.borrow().clone().ok_or_else(not_prepared)
    }
}

impl Lifecycle for PostgresBusinessApprovalPlugin {
    async fn activate(&self, context: ActivateContext) -> Result<(), RuntimeFailure> {
        let database_url = resolve_secret(
            &self.secrets,
            context.dependencies(),
            context.cancellation(),
            &self.config.database_url_secret,
        )
        .await?;
        let postgres = OwnedPostgres::prepare(
            &database_url,
            schema::schema_plan(self.config.schema.clone()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: error.to_string(),
                }
            })?,
        )
        .await
        .map_err(|error| RuntimeFailure::PluginFailure {
            detail: error.to_string(),
        })?;
        self.prepared
            .borrow_mut()
            .replace(PreparedBusinessApproval { postgres });
        Ok(())
    }

    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        let prepared = self.prepared.borrow_mut().take();
        if let Some(prepared) = prepared {
            prepared.postgres.pool().close().await;
        }
        Ok(())
    }
}

async fn resolve_secret(
    secrets: &SecretsClient,
    dependencies: &PluginDependencies,
    cancellation: lenso_kernel::CancellationToken,
    reference: &str,
) -> Result<Zeroizing<String>, RuntimeFailure> {
    let context = dependencies.invocation_context_after(DEPENDENCY_TIMEOUT, cancellation)?;
    secrets
        .resolve_with_context(
            context,
            ResolveRequest {
                reference: reference.to_owned(),
            },
        )
        .await
        .map(|response| Zeroizing::new(response.value))
        .map_err(|error| match error {
            SecretsInvocationError::Domain(_) => RuntimeFailure::PluginFailure {
                detail: format!("database URL secret `{reference}` was rejected"),
            },
            SecretsInvocationError::Runtime(error) => error,
        })
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
fn storage_runtime<E>(error: storage::StorageError) -> PluginError<E> {
    PluginError::runtime(RuntimeFailure::PluginFailure {
        detail: error.to_string(),
    })
}

fn not_prepared() -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: "Business Approval is not prepared".to_owned(),
    }
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

fn validate_callers(callers: &[String]) -> Result<(), CallerListError> {
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

fn valid_request_id(value: &str) -> bool {
    valid_identifier(value, MAX_REQUEST_ID_BYTES)
}

fn valid_idempotency_key(value: &str) -> bool {
    valid_identifier(value, MAX_IDEMPOTENCY_KEY_BYTES)
}

fn valid_instance(value: &str) -> bool {
    valid_identifier(value, 256)
}

fn valid_kind(value: &str) -> bool {
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

fn valid_reason(reason: Option<&str>) -> bool {
    reason.is_none_or(|value| {
        let trimmed = value.trim();
        !trimmed.is_empty()
            && value.len() <= MAX_REASON_BYTES
            && !value.chars().any(char::is_control)
    })
}

fn valid_secret_reference(reference: &str) -> bool {
    !reference.is_empty()
        && reference.len() <= 256
        && !reference.starts_with('/')
        && !reference.ends_with('/')
        && !reference.contains("//")
        && reference
            .split('/')
            .all(|segment| segment != "." && segment != "..")
        && reference.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lenso_app_plan::{AppComposition, PluginInstancePlan};
    use lenso_kernel::{CancellationToken, InvocationContext};
    use lenso_native_adapter::NativePluginRegistry;

    fn config() -> BusinessApprovalConfig {
        BusinessApprovalConfig::new(
            "business_approval",
            "business-approval/database-url",
            vec!["expense-api".to_owned()],
            vec!["approval-console".to_owned()],
            vec!["approval-expirer".to_owned()],
        )
        .unwrap()
    }

    fn plugin() -> PostgresBusinessApprovalPlugin {
        PostgresBusinessApprovalPlugin {
            config: config(),
            secrets: Port::default(),
            prepared: Rc::new(RefCell::new(None)),
        }
    }

    fn context(caller: &str) -> InvocationContext {
        InvocationContext::new(1, None, CancellationToken::new()).with_caller_instance(caller)
    }

    #[test]
    fn descriptor_and_factory_are_macro_generated() {
        let descriptor: serde_json::Value = serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).unwrap();
        assert_eq!(descriptor["plugin_id"], "lenso.business-approval.postgres");
        assert_eq!(
            descriptor["provided_capabilities"][0]["capability_id"],
            approval::CAPABILITY_ID
        );
        assert_eq!(
            descriptor["required_capabilities"][0]["capability_id"],
            secrets::CAPABILITY_ID
        );
        assert_eq!(
            NativePluginRegistry::new()
                .with_linked_factories()
                .factories()
                .filter(|factory| factory.package_id() == PACKAGE_ID)
                .count(),
            1
        );
    }

    #[test]
    fn config_rejects_missing_or_duplicate_exact_callers() {
        let mut invalid = config();
        invalid.requester_instances.clear();
        assert_eq!(
            invalid.validate(),
            Err(BusinessApprovalConfigError::InvalidRequesters(
                CallerListError::EmptyOrTooLarge
            ))
        );
        let mut invalid = config();
        invalid
            .decider_instances
            .push("approval-console".to_owned());
        assert_eq!(
            invalid.validate(),
            Err(BusinessApprovalConfigError::InvalidDeciders(
                CallerListError::DuplicateInstance
            ))
        );
    }

    #[test]
    fn requester_authority_is_exact_and_checked_before_storage() {
        let result = futures::executor::block_on(plugin().request(
            context("expense-api-shadow"),
            RequestRequest {
                request_id: "apr_42".to_owned(),
                idempotency_key: "expense_42".to_owned(),
                requested_by: "usr_requester".to_owned(),
                approval_kind: "expense.review".to_owned(),
                subject: approval::RequestRequestSubject {
                    kind: "expense".to_owned(),
                    id: "exp_42".to_owned(),
                },
                expires_at: "2030-01-01T00:00:00Z".to_owned(),
            },
        ));
        assert_eq!(result, Err(PluginError::Domain(RequestError::Forbidden)));
    }

    #[test]
    fn decision_and_expiration_do_not_inherit_requester_authority() {
        let decision = futures::executor::block_on(plugin().decide(
            context("expense-api"),
            DecideRequest {
                request_id: "apr_42".to_owned(),
                decision: DecideRequestDecision::Approved,
                decided_by: "usr_approver".to_owned(),
                evidence_ref: "decision/42".to_owned(),
                reason: None,
            },
        ));
        assert_eq!(decision, Err(PluginError::Domain(DecideError::Forbidden)));

        let expiration = futures::executor::block_on(plugin().expire(
            context("approval-console"),
            ExpireRequest {
                request_id: "apr_42".to_owned(),
            },
        ));
        assert_eq!(expiration, Err(PluginError::Domain(ExpireError::Forbidden)));
    }

    #[test]
    fn read_is_limited_to_the_union_of_configured_callers() {
        let result = futures::executor::block_on(plugin().read(
            context("unrelated-observer"),
            ReadRequest {
                request_id: "apr_42".to_owned(),
            },
        ));
        assert_eq!(result, Err(PluginError::Domain(ReadError::Forbidden)));
    }

    #[test]
    fn only_deciders_and_expiration_executors_can_read_across_requesters() {
        let config = config();
        assert!(!config.can_read_any_request("expense-api"));
        assert!(config.can_read_any_request("approval-console"));
        assert!(config.can_read_any_request("approval-expirer"));
    }

    #[test]
    fn identifiers_and_reasons_remain_narrow() {
        assert!(valid_request_id("apr_42"));
        assert!(!valid_request_id("apr/42"));
        assert!(valid_kind("expense.review"));
        assert!(!valid_kind("Expense Review"));
        assert!(valid_reason(Some("Within policy")));
        assert!(!valid_reason(Some("   ")));
    }

    #[test]
    fn removing_business_approval_leaves_business_plugins_resolvable() {
        let remaining = AppComposition::new(
            vec![PluginInstancePlan::new("expense", "company.expense")],
            vec![],
        )
        .resolve()
        .expect("business owner does not require Business Approval when removed");
        assert_eq!(remaining.plugin_instances().len(), 1);
        assert!(remaining.capability_bindings().is_empty());
    }
}
