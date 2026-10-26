use async_trait::async_trait;
use serde::Serialize;
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub status: Status,
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Up,
    Down,
}

#[async_trait]
pub trait PostgresHealthChecker: Send + Sync {
    async fn check(&self) -> HealthStatus;
}

#[async_trait]
pub trait RedisHealthChecker: Send + Sync {
    async fn check(&self) -> HealthStatus;
}

#[async_trait]
pub trait KafkaHealthChecker: Send + Sync {
    async fn check(&self) -> HealthStatus;
}

#[derive(Debug, thiserror::Error)]
pub enum HealthError {
    #[error("service unavailable")]
    Unavailable,
}

pub const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(3);
