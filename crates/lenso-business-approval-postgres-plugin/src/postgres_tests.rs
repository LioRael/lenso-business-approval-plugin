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

    let approval_intent = intent(
        "apr_decision",
        "expense-api",
        "expense-42",
        now,
        now + Duration::hours(1),
    );
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
