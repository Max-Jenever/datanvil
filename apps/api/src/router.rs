use std::sync::Arc;
use axum::{routing::{get, post}, Router};

use crate::handlers::health;
use crate::handlers::events;
use crate::state::ApiState;

pub fn build_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/health", get(health::health_aggregated))
        .route("/health/postgres", get(health::health_postgres))
        .route("/health/redis", get(health::health_redis))
        .route("/health/kafka", get(health::health_kafka))
        .route("/v1/events", post(events::create_events))
        .route("/v1/events", get(events::list_events))
        .with_state(state)
}
