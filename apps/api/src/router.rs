use axum::{routing::get, Router};
use std::sync::Arc;

use crate::handlers::health;
use crate::state::ApiState;

pub fn build_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/health", get(health::health_aggregated))
        .route("/health/postgres", get(health::health_postgres))
        .route("/health/redis", get(health::health_redis))
        .route("/health/kafka", get(health::health_kafka))
        .with_state(state)
}
