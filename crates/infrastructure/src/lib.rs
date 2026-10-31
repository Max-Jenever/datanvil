pub mod batch_writer;
pub mod event_repository;
pub mod health;

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
        // Исправлено: query_async принимает только тип возвращаемого значения
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
