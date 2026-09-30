//! D1-backed approval with the same owner request and human-decision policy as PG.
pub mod host_facilities;
use host_facilities::StoreHandle;
use lenso::prelude::*;
use lenso_business_approval_core::{ApprovalPolicy, ApprovalService};
use lenso_capability_business_approval::{
    self as approval, CancelError, CancelRequest, CancelResponse, DecideError, DecideRequest,
    DecideResponse, ExpireError, ExpireRequest, ExpireResponse, ReadError, ReadRequest,
    ReadResponse, RequestError, RequestRequest, RequestResponse,
};
fn validate(config: &ApprovalPolicy) -> Result<(), RuntimeFailure> {
    config.validate()
}
#[lenso::plugin(lifecycle,configuration_schema="configuration.schema.json",validate=validate)]
#[derive(Clone, Debug)]
struct D1BusinessApprovalPlugin {
    #[config]
    config: ApprovalPolicy,
    #[facility(id = "store")]
    store: StoreHandle,
}
impl Lifecycle for D1BusinessApprovalPlugin {
    async fn prepare(&self, _: PrepareContext) -> Result<(), RuntimeFailure> {
        self.store.readiness().await
    }
}
impl D1BusinessApprovalPlugin {
    fn service(&self) -> ApprovalService<StoreHandle> {
        ApprovalService {
            config: self.config.clone(),
            store: self.store.clone(),
        }
    }
}
#[lenso::provides(approval::BusinessApproval)]
impl D1BusinessApprovalPlugin {
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
}
