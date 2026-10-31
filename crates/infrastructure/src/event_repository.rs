use application::event_repository::{EventRepository, EventRepositoryError};
use async_trait::async_trait;
use contracts::EventDto;
use sqlx::PgPool;

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
}
