use contracts::EventDto;
use futures::future::join_all;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::time::Duration;
use tracing::{error, info};

pub struct KafkaEventProducer {
    producer: FutureProducer,
    topic: String,
}

impl KafkaEventProducer {
    pub fn new(brokers: &str, topic: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("batch.num.messages", "10000")
            .set("linger.ms", "5")
            .set("message.timeout.ms", "2000")
            .set("acks", "1")
            .set("queue.buffering.max.messages", "1000000")
            .create()
            .expect("Failed to create Kafka producer");

        Self {
            producer,
            topic: topic.to_string(),
        }
    }

    pub async fn send_batch(&self, events: &[EventDto]) -> usize {
        // 1. Сериализуем все события в owned-данные (Vec<u8>).
        // Эта переменная живет всю функцию, что удовлетворяет borrow checker.
        let serialized_events: Vec<(Vec<u8>, Vec<u8>)> = events
            .iter()
            .filter_map(|event| {
                serde_json::to_vec(event)
                    .ok()
                    .map(|payload| (event.service.clone().into_bytes(), payload))
            })
            .collect();

        let total_to_send = serialized_events.len();
        if total_to_send == 0 {
            return 0;
        }

        let send_futures: Vec<_> = serialized_events
            .iter()
            .map(|(key, payload)| {
                let record = FutureRecord::to(&self.topic).key(key).payload(payload);
                self.producer
                    .send(record, Timeout::After(Duration::from_secs(1)))
            })
            .collect();

        let results = join_all(send_futures).await;

        let failed_count = results.into_iter().filter(|r| r.is_err()).count();
        let success_count = total_to_send - failed_count;

        if failed_count > 0 {
            error!(
                total = total_to_send,
                success = success_count,
                failed = failed_count,
                "Batch send completed with errors"
            );
        } else {
            info!(batch_size = total_to_send, "Successfully flushed batch to Kafka");
        }

        failed_count
    }
}
