use application::health::{HealthStatus, PostgresHealthChecker, Status, HEALTH_CHECK_TIMEOUT};
use async_trait::async_trait;
use sqlx::PgPool;
use std::time::Instant;
use tracing::instrument;

pub struct PgHealthChecker {
    pool: PgPool,
}

impl PgHealthChecker {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PostgresHealthChecker for PgHealthChecker {
    #[instrument(skip(self), fields(component = "postgres"))]
    async fn check(&self) -> HealthStatus {
        let start = Instant::now();

        let result = tokio::time::timeout(
            HEALTH_CHECK_TIMEOUT,
            sqlx::query("SELECT 1").execute(&self.pool),
        )
        .await;

        let latency_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(Ok(_)) => {
                tracing::info!(latency_ms, status = "up", "postgres health check passed");
                HealthStatus {
                    status: Status::Up,
                    latency_ms,
                    error: None,
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(latency_ms, error = %e, "postgres health check failed");
                HealthStatus {
                    status: Status::Down,
                    latency_ms,
                    error: Some(e.to_string()),
                }
            }
            Err(_) => {
                tracing::warn!(latency_ms, "postgres health check timed out");
                HealthStatus {
                    status: Status::Down,
                    latency_ms,
                    error: Some("timeout".to_string()),
                }
            }
        }
    }
}
