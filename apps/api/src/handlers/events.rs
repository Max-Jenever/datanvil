use application::event_repository::EventRepository;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use contracts::{EventDto, EventsResponse};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;

use crate::state::ApiState;

#[derive(Debug, Deserialize)]
pub struct ListEventsQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    100
}

pub async fn create_events(
    State(state): State<Arc<ApiState>>,
    Json(events): Json<Vec<EventDto>>,
) -> impl IntoResponse {
    if events.is_empty() {
        return (StatusCode::BAD_REQUEST, "events array is empty").into_response();
    }

    for event in events {
        if let Err(TrySendError::Full(_)) = state.event_tx.try_send(event) {
            tracing::warn!("Channel is full, returning 503 Service Unavailable");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "system overloaded, please retry later",
            )
                .into_response();
        }
    }

    (StatusCode::ACCEPTED, "events accepted for processing").into_response()
}

pub async fn list_events(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<ListEventsQuery>,
) -> impl IntoResponse {
    let limit = query.limit.clamp(1, 1000);

    match state.event_repo.get_latest_events(limit).await {
        Ok(events) => (
            StatusCode::OK,
            Json(EventsResponse {
                count: events.len(),
                events,
            }),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to fetch events");
            (StatusCode::INTERNAL_SERVER_ERROR, "database error").into_response()
        }
    }
}
