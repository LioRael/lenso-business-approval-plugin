use lenso_postgres_kit::OwnedPostgres;
use sqlx::{AssertSqlSafe, Connection};
use time::{Duration, OffsetDateTime};
use url::Url;

use super::{
    BusinessApprovalOperator, schema,
    storage::{self, ApprovalStatus, DomainFailure, RequestIntent},
};

fn intent(
    request_id: &str,
    requester_instance: &str,
    idempotency_key: &str,
    requested_at: OffsetDateTime,
    expires_at: OffsetDateTime,
) -> RequestIntent {
    RequestIntent {
        intent_digest: None,
        request_id: request_id.to_owned(),
        requester_instance: requester_instance.to_owned(),
        idempotency_key: idempotency_key.to_owned(),
        requested_by: "usr_requester".to_owned(),
        approval_kind: "expense.review".to_owned(),
        subject_kind: "expense".to_owned(),
        subject_id: "exp_42".to_owned(),
        requested_at,
        expires_at,
    }
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn durable_approval_preserves_idempotency_and_single_terminal_evidence() {
    let Some(database_url) = std::env::var("LENSO_BUSINESS_APPROVAL_TEST_DATABASE_URL").ok() else {
        eprintln!(
            "skipping PostgreSQL acceptance; LENSO_BUSINESS_APPROVAL_TEST_DATABASE_URL is unset"
        );
        return;
    };
    let parsed = Url::parse(&database_url).expect("test database URL must be valid");
    let database = parsed.path().trim_start_matches('/');
    assert!(
        database.starts_with("lenso_business_approval_test"),
        "acceptance requires a disposable lenso_business_approval_test* database"
    );

    let schema_name = format!("business_approval_acceptance_{}", std::process::id());
    let mut cleanup = sqlx::PgConnection::connect(&database_url).await.unwrap();
    let drop_schema = format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE");
    sqlx::query(AssertSqlSafe(drop_schema.as_str()))
        .execute(&mut cleanup)
        .await
        .unwrap();
    BusinessApprovalOperator::setup(&database_url, &schema_name)
        .await
        .unwrap();
    let postgres = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.as_str()).unwrap(),
    )
    .await
    .unwrap();
    let now = OffsetDateTime::now_utc().replace_nanosecond(0).unwrap();

    let mut approval_intent = intent(
        "apr_decision",
        "expense-api",
        "expense-42",
        now,
        now + Duration::hours(1),
    );
    approval_intent.intent_digest = Some("a".repeat(64));
    let created = storage::request(&postgres, &approval_intent)
        .await
        .unwrap()
        .unwrap();
    assert!(created.created);
    assert_eq!(created.approval.status, ApprovalStatus::Pending);
    assert_eq!(created.approval.revision, 1);
    assert!(
        storage::read(&postgres, "apr_decision", Some("expense-api"))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        storage::read(&postgres, "apr_decision", Some("another-requester"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        storage::read(&postgres, "apr_decision", None)
            .await
            .unwrap()
            .is_some()
    );

    let repeated = storage::request(&postgres, &approval_intent)
        .await
        .unwrap()
        .unwrap();
    assert!(!repeated.created);
    assert_eq!(repeated.approval.request_id, "apr_decision");
    let mut conflicting = approval_intent.clone();
    conflicting.subject_id = "exp_changed".to_owned();
    assert_eq!(
        storage::request(&postgres, &conflicting).await.unwrap(),
        Err(DomainFailure::IdempotencyConflict)
    );

    let mut changed_digest = approval_intent.clone();
    changed_digest.intent_digest = Some("b".repeat(64));
    assert_eq!(
        storage::request(&postgres, &changed_digest).await.unwrap(),
        Err(DomainFailure::IdempotencyConflict)
    );
    assert_eq!(
        storage::read(&postgres, "apr_decision", Some("expense-api"))
            .await
            .unwrap()
            .unwrap()
            .intent_digest,
        Some("a".repeat(64))
    );

    let decided = storage::decide(
        &postgres,
        "apr_decision",
        ApprovalStatus::Approved,
        "approval-console",
        "usr_approver",
        "decision/expense-42",
        Some("Within policy"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(decided.status, ApprovalStatus::Approved);
    assert_eq!(decided.revision, 2);
    assert_eq!(
        decided.terminal_caller_instance.as_deref(),
        Some("approval-console")
    );
    assert_eq!(decided.terminal_actor.as_deref(), Some("usr_approver"));
    assert_eq!(decided.evidence_ref.as_deref(), Some("decision/expense-42"));
    assert!(decided.terminal_at.is_some());
    assert_eq!(
        storage::cancel(
            &postgres,
            "apr_decision",
            "expense-api",
            "usr_requester",
            None,
        )
        .await
        .unwrap(),
        Err(DomainFailure::AlreadyTerminal)
    );
    let replay_after_terminal = storage::request(&postgres, &approval_intent)
        .await
        .unwrap()
        .unwrap();
    assert!(!replay_after_terminal.created);
    assert_eq!(
        replay_after_terminal.approval.status,
        ApprovalStatus::Approved
    );
    assert_eq!(replay_after_terminal.approval.revision, 2);

    let cancellable = intent(
        "apr_cancel",
        "expense-api",
        "expense-cancel",
        now,
        now + Duration::hours(1),
    );
    storage::request(&postgres, &cancellable)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        storage::cancel(
            &postgres,
            "apr_cancel",
            "another-requester",
            "usr_other",
            None,
        )
        .await
        .unwrap(),
        Err(DomainFailure::NotRequester)
    );
    let cancelled = storage::cancel(
        &postgres,
        "apr_cancel",
        "expense-api",
        "usr_requester",
        Some("No longer needed"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(cancelled.status, ApprovalStatus::Cancelled);
    assert_eq!(cancelled.revision, 2);

    let future = intent(
        "apr_future",
        "expense-api",
        "expense-future",
        now,
        now + Duration::hours(1),
    );
    storage::request(&postgres, &future).await.unwrap().unwrap();
    assert_eq!(
        storage::expire(&postgres, "apr_future", "approval-expirer")
            .await
            .unwrap(),
        Err(DomainFailure::NotDue)
    );

    let overdue = intent(
        "apr_overdue",
        "expense-api",
        "expense-overdue",
        now - Duration::hours(2),
        now - Duration::hours(1),
    );
    storage::request(&postgres, &overdue)
        .await
        .unwrap()
        .unwrap();
    let expired = storage::expire(&postgres, "apr_overdue", "approval-expirer")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired.status, ApprovalStatus::Expired);
    assert_eq!(expired.revision, 2);
    assert_eq!(
        expired.terminal_caller_instance.as_deref(),
        Some("approval-expirer")
    );
    assert!(expired.terminal_actor.is_none());

    postgres.pool().close().await;
    sqlx::query(AssertSqlSafe(
        format!("DROP SCHEMA {schema_name} CASCADE").as_str(),
    ))
    .execute(&mut cleanup)
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn admitted_decider_rejects_forwarded_scoped_user_before_owner_write() {
    use super::{BusinessApprovalConfig, PostgresBusinessApprovalPlugin, PreparedBusinessApproval};
    use lenso::Port;
    use lenso_auth_sdk::{ActorAssertionIssuer, Validity, delegation::SCOPED_DELEGATION_CLAIM};
    use lenso_capability_business_approval as approval;
    use lenso_kernel::{CancellationToken, InvocationContext};
    use std::{cell::RefCell, collections::BTreeMap, rc::Rc};
    let Some(database_url) = std::env::var("LENSO_BUSINESS_APPROVAL_TEST_DATABASE_URL").ok() else {
        eprintln!("requires owned PostgreSQL test URL");
        return;
    };
    assert!(
        Url::parse(&database_url)
            .unwrap()
            .path()
            .trim_start_matches('/')
            .starts_with("lenso_business_approval_test")
    );
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema_name = format!("scoped_human_denial_{unique}");
    BusinessApprovalOperator::setup(&database_url, &schema_name)
        .await
        .unwrap();
    let postgres = OwnedPostgres::prepare(
        &database_url,
        schema::schema_plan(schema_name.as_str()).unwrap(),
    )
    .await
    .unwrap();
    let plugin = PostgresBusinessApprovalPlugin {
        config: BusinessApprovalConfig::new(
            &schema_name,
            "database",
            vec!["test.management/default".into()],
            vec!["test.management/default".into()],
            vec!["test.expirer/default".into()],
        )
        .unwrap(),
        secrets: Port::default(),
        prepared: Rc::new(RefCell::new(Some(PreparedBusinessApproval {
            postgres: postgres.clone(),
        }))),
    };
    let context = || {
        InvocationContext::new(1, None, CancellationToken::new())
            .with_caller_instance("test.management/default")
    };
    let now = OffsetDateTime::now_utc();
    plugin
        .request(
            context(),
            approval::RequestRequest {
                request_id: "apr_scope_guard".into(),
                idempotency_key: "scoped-guard".into(),
                requested_by: "usr_requester".into(),
                approval_kind: "management.write".into(),
                subject: approval::RequestRequestSubject {
                    kind: "management-operation".into(),
                    id: "operation-a".into(),
                },
                intent_digest: Some("a".repeat(64)),
                expires_at: (now + Duration::minutes(5))
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
            },
        )
        .await
        .unwrap();
    let issuer = ActorAssertionIssuer::new("operators.account", b"test-only-owner-key");
    let asserted = |kind: &str, claims| {
        issuer
            .issue(
                "usr_decider",
                kind,
                "authenticated",
                ["lenso.business-approval@1:decide".into()],
                Validity::new(now, now + Duration::minutes(1)).unwrap(),
                claims,
            )
            .attach(context())
            .unwrap()
    };
    let request = || approval::DecideRequest {
        request_id: "apr_scope_guard".into(),
        decided_by: "usr_decider".into(),
        decision: approval::DecideRequestDecision::Approved,
        evidence_ref: "human-consent:scope-guard".into(),
        reason: None,
    };
    for denied in [
        asserted(
            "user",
            BTreeMap::from([(
                SCOPED_DELEGATION_CLAIM.into(),
                serde_json::json!({"task_id":"task-a","agent_session_id":"session-a","delegate_caller":"test.agent/default"}),
            )]),
        ),
        asserted("service_account", BTreeMap::new()),
    ] {
        assert_eq!(
            plugin.decide(denied, request()).await,
            Err(lenso::PluginError::Domain(approval::DecideError::Forbidden))
        );
        let current = storage::read(&postgres, "apr_scope_guard", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, ApprovalStatus::Pending);
        assert_eq!(current.revision, 1);
        assert!(current.terminal_actor.is_none());
    }
    // A verified remote guard may forward a root human; this owner only narrows
    // asserted contexts and retains the exact trusted-local caller boundary.
    plugin
        .decide(asserted("user", BTreeMap::new()), request())
        .await
        .unwrap();
    let current = storage::read(&postgres, "apr_scope_guard", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, ApprovalStatus::Approved);
    assert_eq!(current.terminal_actor.as_deref(), Some("usr_decider"));
    postgres.pool().close().await;
    let mut cleanup = sqlx::PgConnection::connect(&database_url).await.unwrap();
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA \"{schema_name}\" CASCADE"
    )))
    .execute(&mut cleanup)
    .await
    .unwrap();
}
