use application::health::{HealthStatus, KafkaHealthChecker, Status, HEALTH_CHECK_TIMEOUT};
use async_trait::async_trait;
use rdkafka::producer::{FutureProducer, Producer};
use std::time::Instant;
use tracing::instrument;

pub struct KafkaHealthCheckerImpl {
    producer: std::sync::Arc<FutureProducer>,
}

impl KafkaHealthCheckerImpl {
    pub fn new(producer: std::sync::Arc<FutureProducer>) -> Self {
        Self { producer }
    }
}

#[async_trait]
impl KafkaHealthChecker for KafkaHealthCheckerImpl {
    #[instrument(skip(self), fields(component = "kafka"))]
    async fn check(&self) -> HealthStatus {
        let start = Instant::now();
        let producer = self.producer.clone();

        // spawn_blocking возвращает Result<Result<Metadata, KafkaError>, JoinError>
        // Мы оборачиваем это в timeout, получая еще один уровень Result
        let result = tokio::time::timeout(HEALTH_CHECK_TIMEOUT, async move {
            tokio::task::spawn_blocking(move || {
                producer
                    .client()
                    .fetch_metadata(None, std::time::Duration::from_secs(2))
            })
            .await
        })
        .await;

        let latency_ms = start.elapsed().as_millis() as u64;

        match result {
            // Успех: timeout не истек, задача не упала, Kafka ответила успешно
            Ok(Ok(Ok(metadata))) => {
                let broker_count = metadata.brokers().len();
                tracing::info!(
                    latency_ms,
                    broker_count,
                    status = "up",
                    "kafka health check passed"
                );
                HealthStatus {
                    status: Status::Up,
                    latency_ms,
                    error: None,
                }
            }
            // Успех задачи, но ошибка Kafka (например, брокер недоступен)
            Ok(Ok(Err(e))) => {
                tracing::warn!(latency_ms, error = %e, "kafka health check failed");
                HealthStatus {
                    status: Status::Down,
                    latency_ms,
                    error: Some(e.to_string()),
                }
            }
            // Задача spawn_blocking завершилась с ошибкой (паника или отмена)
            Ok(Err(e)) => {
                tracing::warn!(latency_ms, error = %e, "kafka spawn_blocking task failed");
                HealthStatus {
                    status: Status::Down,
                    latency_ms,
                    error: Some(format!("internal task error: {}", e)),
                }
            }
            // Истекло время ожидания (timeout)
            Err(_) => {
                tracing::warn!(latency_ms, "kafka health check timed out");
                HealthStatus {
                    status: Status::Down,
                    latency_ms,
                    error: Some("timeout".to_string()),
                }
            }
        }
    }
}
