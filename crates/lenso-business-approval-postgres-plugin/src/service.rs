use lenso_business_approval_core::{
    ApprovalStore,
    model::{
        ApprovalStatus, DomainFailure, RequestIntent, RequestOutcome, StorageError, StoredApproval,
    },
};
use lenso_postgres_kit::OwnedPostgres;
use time::OffsetDateTime;
#[derive(Clone, Debug)]
pub(crate) struct PostgresStore(pub(crate) Option<OwnedPostgres>);
impl PostgresStore {
    fn prepared(&self) -> Result<&OwnedPostgres, StorageError> {
        self.0.as_ref().ok_or(StorageError::Unavailable)
    }
}
impl ApprovalStore for PostgresStore {
    fn now(&self) -> Result<OffsetDateTime, StorageError> {
        Ok(OffsetDateTime::now_utc())
    }
    async fn request(
        &self,
        intent: &RequestIntent,
    ) -> Result<Result<RequestOutcome, DomainFailure>, StorageError> {
        crate::storage::request(self.prepared()?, intent)
            .await
            .map_err(|_| StorageError::Unavailable)
    }
    async fn read(
        &self,
        id: &str,
        requester: Option<&str>,
    ) -> Result<Option<StoredApproval>, StorageError> {
        crate::storage::read(self.prepared()?, id, requester)
            .await
            .map_err(|_| StorageError::Unavailable)
    }
    async fn decide(
        &self,
        id: &str,
        status: ApprovalStatus,
        caller: &str,
        actor: &str,
        evidence: &str,
        reason: Option<&str>,
    ) -> Result<Result<StoredApproval, DomainFailure>, StorageError> {
        crate::storage::decide(
            self.prepared()?,
            id,
            status,
            caller,
            actor,
            evidence,
            reason,
        )
        .await
        .map_err(|_| StorageError::Unavailable)
    }
    async fn cancel(
        &self,
        id: &str,
        caller: &str,
        actor: &str,
        reason: Option<&str>,
    ) -> Result<Result<StoredApproval, DomainFailure>, StorageError> {
        crate::storage::cancel(self.prepared()?, id, caller, actor, reason)
            .await
            .map_err(|_| StorageError::Unavailable)
    }
    async fn expire(
        &self,
        id: &str,
        caller: &str,
    ) -> Result<Result<StoredApproval, DomainFailure>, StorageError> {
        crate::storage::expire(self.prepared()?, id, caller)
            .await
            .map_err(|_| StorageError::Unavailable)
    }
}
