pub mod health;
pub mod models;
pub mod event_repository;

use app_config::Settings;
use rdkafka::ClientConfig;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

pub struct AppState {
    pub db_pool: sqlx::PgPool,
    pub redis_client: redis::Client,
    pub kafka_producer: Arc<rdkafka::producer::FutureProducer>,
}

impl AppState {
    pub async fn new(settings: &Settings) -> Result<Self, anyhow::Error> {
        // 1. PostgreSQL
        let db_pool = PgPoolOptions::new()
            .max_connections(settings.database.max_connections)
            .connect(&settings.database.url)
            .await?;
        sqlx::query("SELECT 1").execute(&db_pool).await?;

        // 2. Redis
        let redis_client = redis::Client::open(settings.redis.url.as_str())?;
        let mut conn = redis_client.get_multiplexed_async_connection().await?;
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;

        // 3. Kafka
        let kafka_producer: rdkafka::producer::FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &settings.kafka.brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self {
            db_pool,
            redis_client,
            kafka_producer: Arc::new(kafka_producer),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use sqlx::PgPool;
    use crate::models::AggregateRecord;

    // Вспомогательная функция для получения тестового пула
    // Требует установленной переменной окружения DATABASE_URL
    async fn get_test_pool() -> PgPool {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        PgPool::connect(&database_url).await.expect("Failed to connect to test database")
    }

    #[sqlx::test]
    async fn test_aggregate_insert_and_read() {
        let pool = get_test_pool().await;
        let now = Utc::now();
        
        let record = AggregateRecord::new(
            now,
            "auth-service".to_string(),
            "login_latency_ms".to_string(),
            150.5,
        );

        // Вставка данных (UPSERT через ON CONFLICT)
        sqlx::query!(
            r#"
            INSERT INTO aggregates (window_start, service, metric, count, sum, min, max, p99)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (window_start, service, metric) 
            DO UPDATE SET 
                count = aggregates.count + EXCLUDED.count,
                sum = aggregates.sum + EXCLUDED.sum,
                min = LEAST(aggregates.min, EXCLUDED.min),
                max = GREATEST(aggregates.max, EXCLUDED.max),
                p99 = EXCLUDED.p99 -- Упрощенная логика для теста
            "#,
            record.window_start,
            record.service,
            record.metric,
            record.count,
            record.sum,
            record.min,
            record.max,
            record.p99
        )
        .execute(&pool)
        .await
        .expect("Failed to insert aggregate");

        // Чтение данных
        let fetched = sqlx::query_as!(
            AggregateRecord,
            r#"
            SELECT window_start, service, metric, count, sum, min, max, p99
            FROM aggregates
            WHERE service = $1 AND metric = $2 AND window_start = $3
            "#,
            "auth-service",
            "login_latency_ms",
            now
        )
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch aggregate");

        assert_eq!(fetched.service, "auth-service");
        assert_eq!(fetched.count, 1);
        assert!((fetched.sum - 150.5).abs() < f64::EPSILON);
    }
}
