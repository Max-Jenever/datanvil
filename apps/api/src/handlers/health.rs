use application::health::{HealthStatus, Status};
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use std::sync::Arc;

use crate::state::ApiState;

#[derive(Debug, Serialize)]
struct AggregatedHealth {
    pub status: Status,
    pub postgres: HealthStatus,
    pub redis: HealthStatus,
    pub kafka: HealthStatus,
}

pub async fn health_postgres(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let status = state.pg_health.check().await;
    let http_status = if status.status == Status::Up {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (http_status, Json(status))
}

pub async fn health_redis(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let status = state.redis_health.check().await;
    let http_status = if status.status == Status::Up {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (http_status, Json(status))
}

pub async fn health_kafka(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let status = state.kafka_health.check().await;
    let http_status = if status.status == Status::Up {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (http_status, Json(status))
}

pub async fn health_aggregated(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let (postgres, redis, kafka) = tokio::join!(
        state.pg_health.check(),
        state.redis_health.check(),
        state.kafka_health.check(),
    );

    let overall = if postgres.status == Status::Up
        && redis.status == Status::Up
        && kafka.status == Status::Up
    {
        Status::Up
    } else {
        Status::Down
    };

    let http_status = if overall == Status::Up {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let payload = AggregatedHealth {
        status: overall,
        postgres,
        redis,
        kafka,
    };

    (http_status, Json(payload))
}
