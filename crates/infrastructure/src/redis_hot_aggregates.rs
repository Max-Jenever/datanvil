use chrono::{DateTime, Utc};
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotAggregate {
    pub count: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl HotAggregate {
    pub fn new(value: f64) -> Self {
        Self {
            count: 1,
            sum: value,
            min: value,
            max: value,
        }
    }

    pub fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }
}

pub struct RedisHotAggregates {
    conn: MultiplexedConnection,
    ttl_seconds: usize,
}

impl RedisHotAggregates {
    pub fn new(conn: MultiplexedConnection, ttl_seconds: usize) -> Self {
        Self { conn, ttl_seconds }
    }

    pub async fn update(
        &mut self,
        service: &str,
        metric: &str,
        window_start: DateTime<Utc>,
        value: f64,
    ) -> Result<(), redis::RedisError> {
        let key = format!("hot:{}:{}:{}", service, metric, window_start.timestamp());

        let current: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut self.conn)
            .await?;

        let mut agg = if let Some(json) = current {
            serde_json::from_str::<HotAggregate>(&json)
                .unwrap_or_else(|_| HotAggregate::new(value))
        } else {
            HotAggregate::new(value)
        };

        agg.update(value);

        let new_json = serde_json::to_string(&agg).map_err(|e| {
            redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Serialization failed",
                e.to_string(),
            ))
        })?;

        // Явная типизация ::<()> для подавления предупреждения компилятора
        redis::cmd("SET")
            .arg(&key)
            .arg(&new_json)
            .arg("EX")
            .arg(self.ttl_seconds)
            .query_async::<()>(&mut self.conn)
            .await?;

        debug!(key = %key, count = agg.count, "Hot aggregate updated in Redis");
        Ok(())
    }

    pub async fn get_for(
        &mut self,
        service: &str,
        metric: &str,
    ) -> Result<Vec<(DateTime<Utc>, HotAggregate)>, redis::RedisError> {
        let pattern = format!("hot:{}:{}:*", service, metric);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut self.conn)
            .await?;

        let mut result = Vec::new();
        for key in keys {
            let json: Option<String> = redis::cmd("GET")
                .arg(&key)
                .query_async(&mut self.conn)
                .await?;

            if let Some(j) = json {
                if let Ok(agg) = serde_json::from_str::<HotAggregate>(&j) {
                    let parts: Vec<&str> = key.split(':').collect();
                    if parts.len() >= 4 {
                        if let Ok(ts) = parts[3].parse::<i64>() {
                            if let Some(dt) = DateTime::from_timestamp(ts, 0) {
                                result.push((dt, agg));
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }
}
