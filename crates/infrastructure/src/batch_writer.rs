use contracts::EventDto;
use crate::kafka_producer::KafkaEventProducer;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, debug, warn};

pub struct BatchWriter {
    rx: mpsc::Receiver<EventDto>,
    producer: Arc<KafkaEventProducer>,
    batch_size: usize,
    flush_interval: Duration,
}

impl BatchWriter {
    pub fn new(
        rx: mpsc::Receiver<EventDto>,
        producer: Arc<KafkaEventProducer>,
        batch_size: usize,
        flush_interval_ms: u64,
    ) -> Self {
        Self {
            rx,
            producer,
            batch_size,
            flush_interval: Duration::from_millis(flush_interval_ms),
        }
    }

    pub async fn run(mut self) {
        info!(
            batch_size = self.batch_size,
            interval_ms = self.flush_interval.as_millis(),
            "BatchWriter task started (Kafka mode)"
        );

        let mut batch = Vec::with_capacity(self.batch_size);
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                Some(event) = self.rx.recv() => {
                    batch.push(event);
                    
                    // Агрессивно выгребаем канал
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

        let count = batch.len();
        let failed = self.producer.send_batch(batch).await;

        if failed == 0 {
            info!(batch_size = count, "Successfully flushed batch to Kafka");
        } else {
            tracing::error!(
                batch_size = count,
                failed = failed,
                "Partially failed to flush batch to Kafka"
            );
        }

        batch.clear();
    }
}
