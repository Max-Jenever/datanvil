use app_config::get_settings;
use application::event_repository::EventRepository;
use application::health::{KafkaHealthChecker, PostgresHealthChecker, RedisHealthChecker};
use axum::Router;
use infrastructure::batch_writer::BatchWriter;
use infrastructure::event_repository::PostgresEventRepository;
use infrastructure::health::{
    kafka::KafkaHealthCheckerImpl, postgres::PgHealthChecker, redis::RedisHealthCheckerImpl,
};
use infrastructure::AppState;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::info;

mod handlers;
mod router;
mod state;

use state::ApiState;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();
    telemetry::init();

    let settings = get_settings().expect("Failed to load settings");
    info!(?settings, "configuration loaded");

    let infra = AppState::new(&settings).await?;
    info!("infrastructure initialized");

    let (event_tx, event_rx) = mpsc::channel::<contracts::EventDto>(10_000);

    let batch_writer = BatchWriter::new(event_rx, infra.db_pool.clone(), 500, 100);
    tokio::spawn(async move {
        batch_writer.run().await;
    });
    info!("Batch writer started in background");

    let api_state = Arc::new(ApiState {
        pg_health: Arc::new(PgHealthChecker::new(infra.db_pool.clone()))
            as Arc<dyn PostgresHealthChecker>,
        redis_health: Arc::new(RedisHealthCheckerImpl::new(infra.redis_client.clone()))
            as Arc<dyn RedisHealthChecker>,
        kafka_health: Arc::new(KafkaHealthCheckerImpl::new(infra.kafka_producer.clone()))
            as Arc<dyn KafkaHealthChecker>,
        event_tx,
        event_repo: Arc::new(PostgresEventRepository::new(infra.db_pool.clone()))
            as Arc<dyn EventRepository>, // <-- Добавить
    });

    let app: Router = router::build_router(api_state);

    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "HTTP server listening");

    axum::serve(listener, app).await?;

    Ok(())
}
