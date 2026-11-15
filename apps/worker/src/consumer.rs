use contracts::EventDto;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, TopicPartitionList};
use std::time::Duration;
use tracing::{debug, error, info};

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    topic: String,
}

impl KafkaConsumer {
    pub fn new(brokers: &str, topic: &str, group_id: &str) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            // Ручной коммит — ключевое требование этапа
            .set("enable.auto.commit", "false")
            // Начинаем с earliest при первом запуске новой группы
            .set("auto.offset.reset", "earliest")
            // Небольшая задержка для формирования батчей на стороне брокера
            .set("fetch.min.bytes", "1024")
            .set("fetch.wait.max.ms", "500")
            .create()
            .expect("Failed to create Kafka consumer");

        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topic");

        info!(topic = topic, group = group_id, "Kafka consumer subscribed");

        Self {
            consumer,
            topic: topic.to_string(),
        }
    }

    /// Читает батч сообщений: до `max_messages` или до `timeout`.
    /// Возвращает десериализованные события и список TopicPartitionList для коммита.
    pub async fn poll_batch(
        &self,
        max_messages: usize,
        timeout: Duration,
    ) -> (Vec<EventDto>, TopicPartitionList) {
        use futures::StreamExt;

        let mut stream = self.consumer.stream();
        let mut events = Vec::with_capacity(max_messages);
        let mut tpl = TopicPartitionList::new();

        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            if events.len() >= max_messages {
                break;
            }

            let remaining = deadline.duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    let payload = msg.payload_view::<str>();
                    match payload {
                        Some(Ok(s)) => match serde_json::from_str::<EventDto>(s) {
                            Ok(event) => events.push(event),
                            Err(e) => {
                                error!(error = %e, "Failed to deserialize event, skipping");
                            }
                        },
                        Some(Err(e)) => {
                            error!(error = %e, "Failed to read message payload");
                        }
                        None => {
                            error!("Message has no payload");
                        }
                    }
                
                    // Отмечаем последний обработанный оффсет на партицию.
                    // rdkafka::Offset::Offset принимает i64 (следующий ожидаемый оффсет)
                    tpl.add_partition_offset(
                        &self.topic,
                        msg.partition(),
                        rdkafka::Offset::Offset(msg.offset() + 1),
                    ).expect("Failed to add partition offset to TPL");
                }
                Ok(Some(Err(e))) => {
                    error!(error = %e, "Kafka stream error");
                    break;
                }
                Ok(None) => {
                    debug!("Stream ended");
                    break;
                }
                Err(_) => {
                    // Таймаут — возвращаем то, что успели накопить
                    break;
                }
            }
        }

        (events, tpl)
    }

    /// Ручной синхронный коммит оффсетов после успешной обработки батча.
    pub async fn commit(&self, tpl: &TopicPartitionList) -> Result<(), rdkafka::error::KafkaError> {
        self.consumer.commit(tpl, rdkafka::consumer::CommitMode::Sync)?;
        debug!("Offsets committed");
        Ok(())
    }
}
