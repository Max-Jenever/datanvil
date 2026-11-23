use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use contracts::AggregateRecord; // <-- ИСПРАВЛЕНО: импортируем из contracts
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::state::ApiState;

#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    pub service: String,
    pub metric: String,
}

#[derive(Debug, Serialize)]
pub struct MetricWindow {
    pub window_start: String,
    pub source: String, // "redis" | "postgres"
    pub count: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub service: String,
    pub metric: String,
    pub windows: Vec<MetricWindow>,
}

pub async fn get_metrics(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<MetricsQuery>,
) -> impl IntoResponse {
    // 1. Читаем горячие агрегаты из Redis
    let hot_result = state.hot_aggregates.lock().await.get_for(&query.service, &query.metric).await;
    let hot_windows: Vec<MetricWindow> = match hot_result {
        Ok(items) => items
            .into_iter()
            .map(|(ts, agg)| MetricWindow {
                window_start: ts.to_rfc3339(),
                source: "redis".to_string(),
                count: agg.count,
                sum: agg.sum,
                min: agg.min,
                max: agg.max,
            })
            .collect(),
        Err(e) => {
            tracing::error!(error = %e, "Failed to read hot aggregates from Redis");
            Vec::new()
        }
    };

    // 2. Читаем исторические агрегаты из PostgreSQL
    let pg_result = state.event_repo.get_aggregates(&query.service, &query.metric).await;
    let pg_windows: Vec<MetricWindow> = match pg_result {
        Ok(records) => records
            .into_iter()
            .map(|r| MetricWindow {
                window_start: r.window_start.to_rfc3339(),
                source: "postgres".to_string(),
                count: r.count,
                sum: r.sum,
                min: r.min,
                max: r.max,
            })
            .collect(),
        Err(e) => {
            tracing::error!(error = %e, "Failed to read aggregates from PostgreSQL");
            Vec::new()
        }
    };

    // 3. Объединяем: Redis имеет приоритет для тех же окон
    let mut combined: std::collections::HashMap<String, MetricWindow> = std::collections::HashMap::new();

    // Сначала добавляем PG (как базу)
    for w in pg_windows {
        combined.insert(w.window_start.clone(), w);
    }

    // Затем перезаписываем Redis-данными (они свежее)
    for w in hot_windows {
        combined.insert(w.window_start.clone(), w);
    }

    let mut windows: Vec<MetricWindow> = combined.into_values().collect();
    windows.sort_by(|a, b| b.window_start.cmp(&a.window_start));

    (
        StatusCode::OK,
        Json(MetricsResponse {
            service: query.service,
            metric: query.metric,
            windows,
        }),
    )
        .into_response()
}
