use app_config::get_settings;
use application::health::{KafkaHealthChecker, PostgresHealthChecker, RedisHealthChecker};
use axum::Router;
use infrastructure::health::{
    kafka::KafkaHealthCheckerImpl, postgres::PgHealthChecker, redis::RedisHealthCheckerImpl,
};
use infrastructure::AppState;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

mod handlers;
mod router;
mod state;

use state::ApiState;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Загружаем переменные окружения из .env файла
    dotenvy::dotenv().ok();

    telemetry::init();

    let settings = get_settings().expect("Failed to load settings");
    info!(?settings, "configuration loaded");

    let infra = AppState::new(&settings).await?;
    info!("infrastructure initialized");

    let api_state = Arc::new(ApiState {
        pg_health: Arc::new(PgHealthChecker::new(infra.db_pool.clone()))
            as Arc<dyn PostgresHealthChecker>,
        redis_health: Arc::new(RedisHealthCheckerImpl::new(infra.redis_client.clone()))
            as Arc<dyn RedisHealthChecker>,
        kafka_health: Arc::new(KafkaHealthCheckerImpl::new(infra.kafka_producer.clone()))
            as Arc<dyn KafkaHealthChecker>,
    });

    let app: Router = router::build_router(api_state);

    let addr = "0.0.0.0:8080";
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "HTTP server listening");

    axum::serve(listener, app).await?;

    Ok(())
}

