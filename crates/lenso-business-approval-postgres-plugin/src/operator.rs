use lenso_postgres_kit::{PostgresKitError, SchemaOperator, SetupOutcome, UpgradeOutcome};
use thiserror::Error;

use crate::schema::schema_plan;

/// Explicit schema administration for Business Approval.
#[derive(Clone, Copy, Debug, Default)]
pub struct BusinessApprovalOperator;

impl BusinessApprovalOperator {
    /// Creates a missing managed schema and installs the complete authored plan.
    pub async fn setup(
        database_url: &str,
        schema: &str,
    ) -> Result<SetupOutcome, BusinessApprovalOperatorError> {
        Ok(SchemaOperator::connect(database_url, schema_plan(schema)?)
            .await?
            .setup()
            .await?)
    }

    /// Applies pending authored migrations to an existing managed schema.
    pub async fn upgrade(
        database_url: &str,
        schema: &str,
    ) -> Result<UpgradeOutcome, BusinessApprovalOperatorError> {
        Ok(SchemaOperator::connect(database_url, schema_plan(schema)?)
            .await?
            .upgrade()
            .await?)
    }
}

/// Failure from an explicit Business Approval schema workflow.
#[derive(Debug, Error)]
pub enum BusinessApprovalOperatorError {
    #[error(transparent)]
    Plan(#[from] lenso_postgres_kit::PlanError),
    #[error(transparent)]
    Postgres(#[from] PostgresKitError),
}
