use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Модель сырого события для отладки или первичной записи
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EventRaw {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub service: String,
    pub metric: String,
    pub value: f64,
    pub payload: Option<serde_json::Value>,
}

/// Модель агрегированных данных за временное окно
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
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

impl AggregateRecord {
    /// Конструктор для создания новой записи с нуля
    pub fn new(window_start: DateTime<Utc>, service: String, metric: String, value: f64) -> Self {
        Self {
            window_start,
            service,
            metric,
            count: 1,
            sum: value,
            min: value,
            max: value,
            p99: value,
        }
    }
}
