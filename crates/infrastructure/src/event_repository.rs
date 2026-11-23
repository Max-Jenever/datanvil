use application::event_repository::{EventRepository, EventRepositoryError};
use async_trait::async_trait;
use contracts::{AggregateRecord, EventDto};
use sqlx::PgPool;
use sqlx::Row; // <-- ДОБАВЛЕН ЭТОТ ИМПОРТ

pub struct PostgresEventRepository {
    pool: PgPool,
}

impl PostgresEventRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventRepository for PostgresEventRepository {
    async fn insert_events(&self, events: &[EventDto]) -> Result<(), EventRepositoryError> {
        for event in events {
            sqlx::query!(
                r#"
                INSERT INTO events_raw (id, timestamp, service, metric, value, payload)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
                event.id,
                event.timestamp,
                event.service,
                event.metric,
                event.value,
                event.payload
            )
            .execute(&self.pool)
            .await
            .map_err(|e| EventRepositoryError::Database(e.to_string()))?;
        }
        Ok(())
    }

    async fn get_latest_events(&self, limit: i64) -> Result<Vec<EventDto>, EventRepositoryError> {
        let rows = sqlx::query_as!(
            EventDto,
            r#"
            SELECT id, timestamp, service, metric, value, payload
            FROM events_raw
            ORDER BY timestamp DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| EventRepositoryError::Database(e.to_string()))?;

        Ok(rows)
    }

    async fn get_aggregates(&self, service: &str, metric: &str) -> Result<Vec<AggregateRecord>, EventRepositoryError> {
        let rows = sqlx::query(
            "SELECT window_start, service, metric, count, sum, min, max, p99 
             FROM aggregates 
             WHERE service = $1 AND metric = $2 
             ORDER BY window_start DESC",
        )
        .bind(service)
        .bind(metric)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| EventRepositoryError::Database(e.to_string()))?;

        let result = rows.into_iter().map(|row| {
            AggregateRecord {
                window_start: row.get("window_start"),
                service: row.get("service"),
                metric: row.get("metric"),
                count: row.get("count"),
                sum: row.get("sum"),
                min: row.get("min"),
                max: row.get("max"),
                p99: row.get("p99"),
            }
        }).collect();

        Ok(result)
    }
}
