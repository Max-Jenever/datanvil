mod aggregator;
mod consumer;
mod repository;

use aggregator::Aggregator;
use app_config::get_settings;
use consumer::KafkaConsumer;
use repository::AggregateWriter;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();
    telemetry::init();

    let settings = get_settings().expect("Failed to load settings");
    info!(?settings, "Worker configuration loaded");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&settings.database.url)
        .await?;

    let consumer = KafkaConsumer::new(
        &settings.kafka.brokers,
        &settings.kafka.topic_events,
        "aggregator-group",
    );

    let writer = AggregateWriter::new(pool);

    // Окно агрегации 60 секунд. Для тестов удобно уменьшить до 10.
    let mut aggregator = Aggregator::new(60);

    info!("Worker started, entering consume loop");

    loop {
        // 1. Читаем батч: до 1000 сообщений или 5 секунд
        let (events, tpl) = consumer.poll_batch(1000, Duration::from_secs(5)).await;

        if events.is_empty() {
            // Нет новых событий — проверяем закрытие окон по времени
            let now = chrono::Utc::now();
            let closed = aggregator.close_windows(now);
            if !closed.is_empty() {
                writer.upsert(&closed).await?;
                info!(flushed = closed.len(), "Flushed closed windows (idle tick)");
            }
            continue;
        }

        // 2. Агрегируем события в памяти
        for event in &events {
            aggregator.ingest(event);
        }

        // 3. Закрываем истекшие окна и пишем в PostgreSQL
        let now = chrono::Utc::now();
        let closed = aggregator.close_windows(now);
        if !closed.is_empty() {
            writer.upsert(&closed).await?;
            info!(flushed = closed.len(), open = aggregator.open_window_count(), "Flushed closed windows");
        }

        // 4. Ручной коммит оффсетов ТОЛЬКО после успешной обработки
        consumer.commit(&tpl).await?;
        info!(processed = events.len(), "Batch processed and committed");
    }
}
