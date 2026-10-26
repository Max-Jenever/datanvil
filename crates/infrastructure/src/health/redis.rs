use application::health::{HealthStatus, RedisHealthChecker, Status, HEALTH_CHECK_TIMEOUT};
use async_trait::async_trait;
use redis::Client;
use std::time::Instant;
use tracing::instrument;

pub struct RedisHealthCheckerImpl {
    client: Client,
}

impl RedisHealthCheckerImpl {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RedisHealthChecker for RedisHealthCheckerImpl {
    #[instrument(skip(self), fields(component = "redis"))]
    async fn check(&self) -> HealthStatus {
        let start = Instant::now();

        let result = tokio::time::timeout(HEALTH_CHECK_TIMEOUT, async {
            let mut conn = self.client.get_multiplexed_async_connection().await?;
            // Исправлено: указываем только тип возвращаемого значения String
            redis::cmd("PING").query_async::<String>(&mut conn).await
        })
        .await;

        let latency_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(Ok(pong)) if pong == "PONG" => {
                tracing::info!(latency_ms, status = "up", "redis health check passed");
                HealthStatus {
                    status: Status::Up,
                    latency_ms,
                    error: None,
                }
            }
            Ok(Ok(response)) => {
                tracing::warn!(latency_ms, response, "redis returned unexpected response");
                HealthStatus {
                    status: Status::Down,
                    latency_ms,
                    error: Some(format!("unexpected response: {response}")),
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(latency_ms, error = %e, "redis health check failed");
                HealthStatus {
                    status: Status::Down,
                    latency_ms,
                    error: Some(e.to_string()),
                }
            }
            Err(_) => {
                tracing::warn!(latency_ms, "redis health check timed out");
                HealthStatus {
                    status: Status::Down,
                    latency_ms,
                    error: Some("timeout".to_string()),
                }
            }
        }
    }
}
