use contracts::EventDto;
use crate::kafka_producer::KafkaEventProducer;
use crate::redis_dedup::RedisDeduplication;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::{info, debug, warn};

pub struct BatchWriter {
    rx: mpsc::Receiver<EventDto>,
    producer: Arc<KafkaEventProducer>,
    dedup: Arc<Mutex<RedisDeduplication>>,
    batch_size: usize,
    flush_interval: Duration,
}

impl BatchWriter {
    pub fn new(
        rx: mpsc::Receiver<EventDto>,
        producer: Arc<KafkaEventProducer>,
        dedup: Arc<Mutex<RedisDeduplication>>,
        batch_size: usize,
        flush_interval_ms: u64,
    ) -> Self {
        Self {
            rx,
            producer,
            dedup,
            batch_size,
            flush_interval: Duration::from_millis(flush_interval_ms),
        }
    }

    pub async fn run(mut self) {
        info!(
            batch_size = self.batch_size,
            interval_ms = self.flush_interval.as_millis(),
            "BatchWriter task started (Kafka + dedup mode)"
        );

        let mut batch = Vec::with_capacity(self.batch_size);
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                Some(event) = self.rx.recv() => {
                    batch.push(event);
                    while batch.len() < self.batch_size {
                        match self.rx.try_recv() {
                            Ok(e) => batch.push(e),
                            Err(_) => break,
                        }
                    }
                    if batch.len() >= self.batch_size {
                        self.flush(&mut batch).await;
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        debug!("Timer triggered, flushing partial batch of size {}", batch.len());
                        self.flush(&mut batch).await;
                    }
                }
                else => {
                    warn!("Channel closed, flushing remaining {} events and exiting", batch.len());
                    if !batch.is_empty() {
                        self.flush(&mut batch).await;
                    }
                    break;
                }
            }
        }
    }

    async fn flush(&self, batch: &mut Vec<EventDto>) {
        if batch.is_empty() {
            return;
        }

        let total = batch.len();
        let mut dedup_guard = self.dedup.lock().await;
        let mut unique_events = Vec::with_capacity(total);

        for event in batch.drain(..) {
            let event_id = event.id.to_string();
            match dedup_guard.try_mark_seen(&event_id).await {
                Ok(true) => unique_events.push(event),
                Ok(false) => { /* дубликат, пропущен */ }
                Err(e) => {
                    tracing::error!(error = %e, event_id = %event_id, "Dedup check failed, passing event through");
                    unique_events.push(event);
                }
            }
        }
        drop(dedup_guard);

        let dropped = total - unique_events.len();
        if dropped > 0 {
            info!(total = total, dropped = dropped, kept = unique_events.len(), "Deduplication filter applied");
        }

        if !unique_events.is_empty() {
            let failed = self.producer.send_batch(&unique_events).await;
            if failed == 0 {
                info!(batch_size = unique_events.len(), "Successfully flushed batch to Kafka");
            } else {
                tracing::error!(
                    batch_size = unique_events.len(),
                    failed = failed,
                    "Partially failed to flush batch to Kafka"
                );
            }
        }
    }
}
