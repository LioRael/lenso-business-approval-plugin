//! A finite private owner-store handle. Submitted transition failures remain Runtime unknown.
use lenso::prelude::*;
use lenso_business_approval_core::{
    ApprovalStore,
    model::{
        ApprovalStatus, DomainFailure, RequestIntent, RequestOutcome, StorageError, StoredApproval,
    },
};
use time::OffsetDateTime;
#[derive(Clone)]
pub struct StoreHandle {
    #[cfg(target_arch = "wasm32")]
    inner: wasm_bindgen::JsValue,
}
impl std::fmt::Debug for StoreHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApprovalD1Store").finish_non_exhaustive()
    }
}
#[cfg(not(target_arch = "wasm32"))]
pub fn store(_: &serde_json::Value) -> Result<StoreHandle, RuntimeFailure> {
    Err(RuntimeFailure::InvalidResolvedPlan {
        detail: "D1 Approval requires Workers D1".into(),
    })
}
#[cfg(target_arch = "wasm32")]
pub fn store(value: &wasm_bindgen::JsValue) -> Result<StoreHandle, RuntimeFailure> {
    if !value.is_object() {
        return Err(unavailable());
    }
    Ok(StoreHandle {
        inner: value.clone(),
    })
}
fn unavailable() -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: "D1 Approval storage unavailable; submitted transitions may have committed".into(),
    }
}
impl StoreHandle {
    #[cfg(target_arch = "wasm32")]
    pub async fn readiness(&self) -> Result<(), RuntimeFailure> {
        #[cfg(target_arch = "wasm32")]
        {
            let _: bool = self
                .call("readiness", &())
                .await
                .map_err(|_| unavailable())?;
            Ok(())
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            Err(unavailable())
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub fn readiness(&self) -> impl std::future::Future<Output = Result<(), RuntimeFailure>> {
        std::future::ready(Err(unavailable()))
    }

    #[cfg(target_arch = "wasm32")]
    async fn call<I: serde::Serialize, O: serde::de::DeserializeOwned>(
        &self,
        name: &str,
        input: &I,
    ) -> Result<O, StorageError> {
        use wasm_bindgen::JsCast;
        let f = js_sys::Reflect::get(&self.inner, &wasm_bindgen::JsValue::from_str(name))
            .map_err(|_| StorageError::Unavailable)?
            .dyn_into::<js_sys::Function>()
            .map_err(|_| StorageError::Unavailable)?;
        let input =
            serde::Serialize::serialize(input, &serde_wasm_bindgen::Serializer::json_compatible())
                .map_err(|_| StorageError::Unavailable)?;
        let value = f
            .call1(&self.inner, &input)
            .map_err(|_| StorageError::Unavailable)?;
        let result = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&value))
            .await
            .map_err(|_| StorageError::Unavailable)?;
        serde_wasm_bindgen::from_value(result).map_err(|_| StorageError::Unavailable)
    }
    #[cfg(target_arch = "wasm32")]
    async fn transition(
        &self,
        name: &str,
        input: serde_json::Value,
    ) -> Result<Result<StoredApproval, DomainFailure>, StorageError> {
        let result: TransitionResult = self.call(name, &input).await?;
        if let Some(approval) = &result.approval {
            lenso_business_approval_core::model::validate_evidence(approval)?;
        }
        if result.changed {
            return result.approval.ok_or(StorageError::Unavailable).map(Ok);
        }
        let Some(approval) = result.approval else {
            return Ok(Err(DomainFailure::RequestNotFound));
        };
        if name == "cancel" && approval.requester_instance != input["caller"].as_str().unwrap_or("")
        {
            return Ok(Err(DomainFailure::NotRequester));
        }
        if approval.status != ApprovalStatus::Pending {
            return Ok(Err(DomainFailure::AlreadyTerminal));
        }
        if name == "expire" {
            return Ok(Err(DomainFailure::NotDue));
        }
        Err(StorageError::Unavailable)
    }
}
#[cfg(target_arch = "wasm32")]
#[derive(serde::Deserialize)]
struct TransitionResult {
    changed: bool,
    approval: Option<StoredApproval>,
}

#[cfg(target_arch = "wasm32")]
impl ApprovalStore for StoreHandle {
    fn now(&self) -> Result<OffsetDateTime, StorageError> {
        #[cfg(target_arch = "wasm32")]
        {
            use wasm_bindgen::JsCast;
            let f = js_sys::Reflect::get(&self.inner, &wasm_bindgen::JsValue::from_str("now_us"))
                .map_err(|_| StorageError::Unavailable)?
                .dyn_into::<js_sys::Function>()
                .map_err(|_| StorageError::Unavailable)?;
            let micros = f
                .call0(&self.inner)
                .map_err(|_| StorageError::Unavailable)?
                .as_string()
                .and_then(|s| s.parse::<i128>().ok())
                .ok_or(StorageError::Unavailable)?;
            OffsetDateTime::from_unix_timestamp_nanos(
                micros.checked_mul(1000).ok_or(StorageError::Unavailable)?,
            )
            .map_err(|_| StorageError::Unavailable)
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            Err(StorageError::Unavailable)
        }
    }
    async fn request(
        &self,
        intent: &RequestIntent,
    ) -> Result<Result<RequestOutcome, DomainFailure>, StorageError> {
        #[cfg(target_arch = "wasm32")]
        {
            #[derive(serde::Deserialize)]
            struct ResultRows {
                created: bool,
                rows: Vec<StoredApproval>,
            }
            let input = serde_json::json!({"intent":intent,"expires_us":(intent.expires_at.unix_timestamp_nanos()/1000).to_string()});
            let result: ResultRows = self.call("request", &input).await?;
            if result.rows.len() != 1 {
                return Ok(Err(DomainFailure::IdempotencyConflict));
            }
            let approval = result
                .rows
                .into_iter()
                .next()
                .ok_or(StorageError::InconsistentIdempotency)?;
            lenso_business_approval_core::model::validate_evidence(&approval)?;
            if !lenso_business_approval_core::model::same_intent(&approval, intent) {
                return Ok(Err(DomainFailure::IdempotencyConflict));
            }
            Ok(Ok(RequestOutcome {
                created: result.created,
                approval,
            }))
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = intent;
            Err(StorageError::Unavailable)
        }
    }
    async fn read(
        &self,
        id: &str,
        requester: Option<&str>,
    ) -> Result<Option<StoredApproval>, StorageError> {
        #[cfg(target_arch = "wasm32")]
        {
            let approval: Option<StoredApproval> = self
                .call("read", &serde_json::json!({"id":id,"requester":requester}))
                .await?;
            if let Some(a) = &approval {
                lenso_business_approval_core::model::validate_evidence(a)?;
            }
            Ok(approval)
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (id, requester);
            Err(StorageError::Unavailable)
        }
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
        #[cfg(target_arch = "wasm32")]
        {
            self.transition("decide",serde_json::json!({"id":id,"status":status,"caller":caller,"actor":actor,"evidence":evidence,"reason":reason})).await
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (id, status, caller, actor, evidence, reason);
            Err(StorageError::Unavailable)
        }
    }
    async fn cancel(
        &self,
        id: &str,
        caller: &str,
        actor: &str,
        reason: Option<&str>,
    ) -> Result<Result<StoredApproval, DomainFailure>, StorageError> {
        #[cfg(target_arch = "wasm32")]
        {
            self.transition(
                "cancel",
                serde_json::json!({"id":id,"caller":caller,"actor":actor,"reason":reason}),
            )
            .await
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (id, caller, actor, reason);
            Err(StorageError::Unavailable)
        }
    }
    async fn expire(
        &self,
        id: &str,
        caller: &str,
    ) -> Result<Result<StoredApproval, DomainFailure>, StorageError> {
        #[cfg(target_arch = "wasm32")]
        {
            self.transition("expire", serde_json::json!({"id":id,"caller":caller}))
                .await
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (id, caller);
            Err(StorageError::Unavailable)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ApprovalStore for StoreHandle {
    fn now(&self) -> Result<OffsetDateTime, StorageError> {
        Err(StorageError::Unavailable)
    }
    fn request(
        &self,
        _intent: &RequestIntent,
    ) -> impl std::future::Future<Output = Result<Result<RequestOutcome, DomainFailure>, StorageError>>
    {
        std::future::ready(Err(StorageError::Unavailable))
    }
    fn read(
        &self,
        _id: &str,
        _requester: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Option<StoredApproval>, StorageError>> {
        std::future::ready(Err(StorageError::Unavailable))
    }
    fn decide(
        &self,
        _id: &str,
        _status: ApprovalStatus,
        _caller: &str,
        _actor: &str,
        _evidence: &str,
        _reason: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Result<StoredApproval, DomainFailure>, StorageError>>
    {
        std::future::ready(Err(StorageError::Unavailable))
    }
    fn cancel(
        &self,
        _id: &str,
        _caller: &str,
        _actor: &str,
        _reason: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Result<StoredApproval, DomainFailure>, StorageError>>
    {
        std::future::ready(Err(StorageError::Unavailable))
    }
    fn expire(
        &self,
        _id: &str,
        _caller: &str,
    ) -> impl std::future::Future<Output = Result<Result<StoredApproval, DomainFailure>, StorageError>>
    {
        std::future::ready(Err(StorageError::Unavailable))
    }
}
