use application::event_repository::EventRepository;
use application::health::{KafkaHealthChecker, PostgresHealthChecker, RedisHealthChecker};
use contracts::EventDto;
use infrastructure::redis_hot_aggregates::RedisHotAggregates;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

pub struct ApiState {
    pub pg_health: Arc<dyn PostgresHealthChecker>,
    pub redis_health: Arc<dyn RedisHealthChecker>,
    pub kafka_health: Arc<dyn KafkaHealthChecker>,
    pub event_tx: mpsc::Sender<EventDto>,
    pub event_repo: Arc<dyn EventRepository>,
    pub hot_aggregates: Arc<Mutex<RedisHotAggregates>>,
}
