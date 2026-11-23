use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDto {
    #[serde(default = "Uuid::new_v4")]
    pub id: Uuid,
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
    pub service: String,
    pub metric: String,
    pub value: f64,
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct EventsResponse {
    pub count: usize,
    pub events: Vec<EventDto>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregateRecord {
    pub window_start: DateTime<Utc>,
    pub service: String,
    pub metric: String,
    pub count: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub p99: f64,
}
