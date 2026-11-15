use crate::aggregator::{AggregateKey, AggregateState};
use sqlx::PgPool;

pub struct AggregateWriter {
    pool: PgPool,
}

impl AggregateWriter {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Пакетный upsert закрытых окон.
    pub async fn upsert(&self, records: &[(AggregateKey, AggregateState)]) -> Result<(), sqlx::Error> {
        if records.is_empty() {
            return Ok(());
        }

        let mut query_str = String::from(
            "INSERT INTO aggregates (window_start, service, metric, count, sum, min, max, p99) VALUES ",
        );

        for (i, _) in records.iter().enumerate() {
            if i > 0 {
                query_str.push_str(", ");
            }
            let base = i * 8 + 1;
            query_str.push_str(&format!(
                "(${base}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                base + 1, base + 2, base + 3, base + 4, base + 5, base + 6, base + 7
            ));
        }

        query_str.push_str(
            " ON CONFLICT (window_start, service, metric) DO UPDATE SET \
             count = EXCLUDED.count, \
             sum = EXCLUDED.sum, \
             min = EXCLUDED.min, \
             max = EXCLUDED.max, \
             p99 = EXCLUDED.p99",
        );

        let mut query = sqlx::query(&query_str);
        for (key, state) in records {
            query = query
                .bind(key.window_start)
                .bind(&key.service)
                .bind(&key.metric)
                .bind(state.count)
                .bind(state.sum)
                .bind(state.min)
                .bind(state.max)
                .bind(state.max); // p99 = max (упрощение)
        }

        // .map(|_| ()) преобразует Result<PgQueryResult, Error> в Result<(), Error>
        query.execute(&self.pool).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregator::AggregateKey;
    use chrono::{Duration, TimeZone, Utc};

    #[sqlx::test]
    async fn test_upsert_writes_and_updates(pool: PgPool) {
        let writer = AggregateWriter::new(pool.clone());
        let base = Utc.with_ymd_and_hms(2026, 10, 28, 14, 30, 0).unwrap();
        let key = AggregateKey {
            service: "test_service".to_string(),
            metric: "test_metric".to_string(),
            window_start: base,
        };
        let state = AggregateState {
            count: 5,
            sum: 500.0,
            min: 50.0,
            max: 150.0,
        };

        writer.upsert(&[(key.clone(), state.clone())]).await.unwrap();

        let row: (i64, f64, f64, f64) = sqlx::query_as(
            "SELECT count, sum, min, max FROM aggregates WHERE service = $1 AND metric = $2 AND window_start = $3",
        )
        .bind(&key.service)
        .bind(&key.metric)
        .bind(key.window_start)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.0, 5);
        assert_eq!(row.1, 500.0);
        assert_eq!(row.2, 50.0);
        assert_eq!(row.3, 150.0);

        // Повторный upsert должен обновить строку, а не создать дубликат
        let state2 = AggregateState {
            count: 10,
            sum: 1000.0,
            min: 10.0,
            max: 300.0,
        };
        writer.upsert(&[(key.clone(), state2)]).await.unwrap();

        let row2: (i64,) = sqlx::query_as(
            "SELECT count FROM aggregates WHERE service = $1 AND metric = $2 AND window_start = $3",
        )
        .bind(&key.service)
        .bind(&key.metric)
        .bind(key.window_start)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row2.0, 10);

        // Очистка тестовых данных
        sqlx::query("DELETE FROM aggregates WHERE service = $1")
            .bind(&key.service)
            .execute(&pool)
            .await
            .unwrap();
    }
}
