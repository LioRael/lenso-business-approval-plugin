//! PostgreSQL-backed independent Business Approval Plugin.

mod operator;
#[cfg(all(test, feature = "postgres-acceptance"))]
mod postgres_tests;
mod schema;
mod service;
mod storage;

use std::{cell::RefCell, fmt, rc::Rc, time::Duration};

use lenso::prelude::*;
pub use lenso_business_approval_core::CallerListError;
use lenso_business_approval_core::validate_callers;
#[cfg(test)]
use lenso_business_approval_core::{valid_kind, valid_reason, valid_request_id};
use lenso_capability_business_approval as approval;
#[cfg(test)]
use lenso_capability_business_approval::DecideRequestDecision;
use lenso_capability_business_approval::{
    CancelError, CancelRequest, CancelResponse, DecideError, DecideRequest, DecideResponse,
    ExpireError, ExpireRequest, ExpireResponse, ReadError, ReadRequest, ReadResponse, RequestError,
    RequestRequest, RequestResponse,
};
use lenso_capability_secrets as secrets;
use lenso_capability_secrets::{ResolveRequest, SecretsClient, SecretsInvocationError};
use lenso_kernel::{PluginDependencies, RuntimeFailure};
use lenso_postgres_kit::OwnedPostgres;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

pub use operator::{BusinessApprovalOperator, BusinessApprovalOperatorError};

const DEPENDENCY_TIMEOUT: Duration = Duration::from_secs(10);
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

    #[cfg(test)]
    fn can_request(&self, caller: &str) -> bool {
        contains_exact(&self.requester_instances, caller)
    }

    #[cfg(test)]
    fn can_decide(&self, caller: &str) -> bool {
        contains_exact(&self.decider_instances, caller)
    }

    #[cfg(test)]
    fn can_expire(&self, caller: &str) -> bool {
        contains_exact(&self.expiration_executor_instances, caller)
    }

    #[cfg(test)]
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
        self.service().request(context, request).await
    }
    async fn decide(
        &self,
        context: Ctx,
        request: DecideRequest,
    ) -> PluginResult<DecideResponse, DecideError> {
        self.service().decide(context, request).await
    }
    async fn cancel(
        &self,
        context: Ctx,
        request: CancelRequest,
    ) -> PluginResult<CancelResponse, CancelError> {
        self.service().cancel(context, request).await
    }
    async fn read(
        &self,
        context: Ctx,
        request: ReadRequest,
    ) -> PluginResult<ReadResponse, ReadError> {
        self.service().read(context, request).await
    }
    async fn expire(
        &self,
        context: Ctx,
        request: ExpireRequest,
    ) -> PluginResult<ExpireResponse, ExpireError> {
        self.service().expire(context, request).await
    }
    fn service(&self) -> lenso_business_approval_core::ApprovalService<service::PostgresStore> {
        lenso_business_approval_core::ApprovalService {
            config: lenso_business_approval_core::ApprovalPolicy {
                requester_instances: self.config.requester_instances.clone(),
                decider_instances: self.config.decider_instances.clone(),
                expiration_executor_instances: self.config.expiration_executor_instances.clone(),
            },
            store: service::PostgresStore(
                self.prepared.borrow().as_ref().map(|p| p.postgres.clone()),
            ),
        }
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

#[cfg(test)]
fn contains_exact(callers: &[String], caller: &str) -> bool {
    callers.iter().any(|allowed| allowed == caller)
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
    fn configuration_schema_resolves_and_owner_formats_remain_strict() {
        use lenso_app_plan::{
            CapabilityEndpointPlan,
            authoring::{
                HostBinding, HostCatalog, HostDefaultPlugin, HostPluginRelease, HostSlot,
                PluginDescriptor, PluginInstanceId, PluginRootSnapshot, resolve_plugin_root,
            },
        };

        let descriptor = serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).unwrap();
        let secrets_provider = PluginDescriptor::new("test.secrets", "0.1.0", "secrets")
            .with_capability(CapabilityEndpointPlan::new(
                secrets::CAPABILITY_ID,
                secrets::DESCRIPTOR_VERSION,
                ["resolve"],
            ));
        let host = HostCatalog::new(
            [HostSlot::one("business-approval"), HostSlot::one("secrets")],
            [
                HostPluginRelease::new(descriptor),
                HostPluginRelease::new(secrets_provider),
            ],
            [
                HostDefaultPlugin::new(PACKAGE_ID, "default")
                    .with_configuration(serde_json::to_value(config()).unwrap()),
                HostDefaultPlugin::new("test.secrets", "default"),
            ],
        )
        .with_bindings([HostBinding::to_instance(
            PluginInstanceId::new(PACKAGE_ID, "default"),
            secrets::CAPABILITY_ID,
            PluginInstanceId::new("test.secrets", "default"),
        )]);
        resolve_plugin_root(&host, &PluginRootSnapshot::default()).unwrap();
        validate_config(&config()).unwrap();

        for (field, value) in [
            ("schema", serde_json::json!("bad;schema")),
            ("database_url_secret", serde_json::json!("../database")),
            (
                "requester_instances",
                serde_json::json!(["lenso.management/default/other"]),
            ),
            ("decider_instances", serde_json::json!(["/default"])),
            (
                "expiration_executor_instances",
                serde_json::json!(["invalid caller"]),
            ),
        ] {
            let mut encoded = serde_json::to_value(config()).unwrap();
            encoded[field] = value;
            let invalid = serde_json::from_value(encoded).unwrap();
            assert!(validate_config(&invalid).is_err(), "{field}");
        }
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
    fn canonical_instance_callers_retain_exact_authority() {
        let mut policy = config();
        policy.requester_instances = vec!["lenso.management/main".into()];
        policy.decider_instances = policy.requester_instances.clone();
        policy.expiration_executor_instances = policy.requester_instances.clone();
        policy.validate().unwrap();
        assert!(policy.can_request("lenso.management/main"));
        assert!(policy.can_decide("lenso.management/main"));
        assert!(!policy.can_request("lenso.management"));
        assert!(!policy.can_decide("lenso.management/other"));
        for invalid in [
            "",
            "/main",
            "lenso.management/",
            "lenso.management//main",
            "lenso.management/main/other",
        ] {
            policy.requester_instances = vec![invalid.into()];
            assert_eq!(
                policy.validate(),
                Err(BusinessApprovalConfigError::InvalidRequesters(
                    CallerListError::InvalidInstance
                ))
            );
        }
    }

    #[test]
    fn requester_authority_is_exact_and_checked_before_storage() {
        let result = futures::executor::block_on(plugin().request(
            context("expense-api-shadow"),
            RequestRequest {
                intent_digest: None,
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
