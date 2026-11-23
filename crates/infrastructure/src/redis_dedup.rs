use redis::aio::MultiplexedConnection;
use tracing::debug;

pub struct RedisDeduplication {
    conn: MultiplexedConnection,
    ttl_seconds: usize,
    dropped_count: u64,
}

impl RedisDeduplication {
    pub fn new(conn: MultiplexedConnection, ttl_seconds: usize) -> Self {
        Self {
            conn,
            ttl_seconds,
            dropped_count: 0,
        }
    }

    pub async fn try_mark_seen(&mut self, event_id: &str) -> Result<bool, redis::RedisError> {
        let key = format!("dedup:{}", event_id);
        
        let result: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg("1")
            .arg("NX")
            .arg("EX")
            .arg(self.ttl_seconds)
            .query_async(&mut self.conn)
            .await?;

        match result {
            Some(_) => {
                debug!(event_id = event_id, "New event marked in dedup cache");
                Ok(true)
            }
            None => {
                self.dropped_count += 1;
                debug!(
                    event_id = event_id,
                    total_dropped = self.dropped_count,
                    "Duplicate event detected and dropped"
                );
                Ok(false)
            }
        }
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }
}
