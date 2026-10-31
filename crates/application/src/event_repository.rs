use async_trait::async_trait;
use contracts::EventDto;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EventRepositoryError {
    #[error("database error: {0}")]
    Database(String),
    #[error("validation error: {0}")]
    Validation(String),
}

#[async_trait]
pub trait EventRepository: Send + Sync {
    async fn insert_events(&self, events: &[EventDto]) -> Result<(), EventRepositoryError>;
    async fn get_latest_events(&self, limit: i64) -> Result<Vec<EventDto>, EventRepositoryError>;
}
