use application::health::{KafkaHealthChecker, PostgresHealthChecker, RedisHealthChecker};
use std::sync::Arc;

pub struct ApiState {
    pub pg_health: Arc<dyn PostgresHealthChecker>,
    pub redis_health: Arc<dyn RedisHealthChecker>,
    pub kafka_health: Arc<dyn KafkaHealthChecker>,
}
