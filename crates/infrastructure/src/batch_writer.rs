use contracts::EventDto;
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

pub struct BatchWriter {
    rx: mpsc::Receiver<EventDto>,
    pool: PgPool,
    batch_size: usize,
    flush_interval: Duration,
}

impl BatchWriter {
    pub fn new(
        rx: mpsc::Receiver<EventDto>,
        pool: PgPool,
        batch_size: usize,
        flush_interval_ms: u64,
    ) -> Self {
        Self {
            rx,
            pool,
            batch_size,
            flush_interval: Duration::from_millis(flush_interval_ms),
        }
    }

    pub async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut interval = tokio::time::interval(self.flush_interval);
        // Игнорируем пропущенные тики, если обработка заняла слишком много времени
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                // Условие 1: Пришло новое событие
                Some(event) = self.rx.recv() => {
                    batch.push(event);
                    if batch.len() >= self.batch_size {
                        self.flush(&mut batch).await;
                    }
                }
                // Условие 2: Истекло время
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        self.flush(&mut batch).await;
                    }
                }
                // Условие 3: Канал закрыт (завершение работы приложения)
                else => {
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

        // Формирование динамического Bulk INSERT запроса
        // sqlx::query! не поддерживает динамическое число аргументов, поэтому используем sqlx::query
        let mut query_str = String::from(
            "INSERT INTO events_raw (id, timestamp, service, metric, value, payload) VALUES ",
        );

        for (i, _) in batch.iter().enumerate() {
            if i > 0 {
                query_str.push_str(", ");
            }
            // Генерируем плейсхолдеры: ($1, $2, $3, $4, $5, $6), ($7, $8, ...)
            let base = i * 6 + 1;
            query_str.push_str(&format!(
                "(${base}, ${}, ${}, ${}, ${}, ${})",
                base + 1,
                base + 2,
                base + 3,
                base + 4,
                base + 5
            ));
        }

        let mut query = sqlx::query(&query_str);

        // Привязка аргументов
        for event in batch.iter() {
            query = query
                .bind(event.id)
                .bind(event.timestamp)
                .bind(&event.service)
                .bind(&event.metric)
                .bind(event.value)
                .bind(&event.payload);
        }

        match query.execute(&self.pool).await {
            Ok(_) => {
                info!(
                    batch_size = count,
                    "Successfully flushed batch to PostgreSQL"
                );
            }
            Err(e) => {
                error!(error = %e, batch_size = count, "Failed to flush batch to PostgreSQL");
            }
        }

        // Очистка вектора без освобождения памяти (переиспользование capacity)
        batch.clear();
    }
}
