use chrono::{DateTime, Duration, Utc};
use contracts::EventDto;
use std::collections::HashMap;

/// Ключ агрегации: (service, metric, window_start)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateKey {
    pub service: String,
    pub metric: String,
    pub window_start: DateTime<Utc>,
}

/// Промежуточный агрегат в памяти
#[derive(Debug, Clone)]
pub struct AggregateState {
    pub count: i64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl AggregateState {
    pub fn new(value: f64) -> Self {
        Self {
            count: 1,
            sum: value,
            min: value,
            max: value,
        }
    }

    pub fn add(&mut self, value: f64) {
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

/// Агрегатор с tumbling-окнами фиксированного размера
pub struct Aggregator {
    window_size: Duration,
    /// Окна, которые еще не закрыты. Ключ -> (состояние, список сырых значений для p99)
    open_windows: HashMap<AggregateKey, AggregateState>,
}

impl Aggregator {
    pub fn new(window_size_seconds: i64) -> Self {
        Self {
            window_size: Duration::seconds(window_size_seconds),
            open_windows: HashMap::new(),
        }
    }

    /// Округляет timestamp события до начала окна (tumbling window)
    fn window_start_of(&self, ts: DateTime<Utc>) -> DateTime<Utc> {
        let secs = ts.timestamp();
        let window_secs = self.window_size.num_seconds();
        let aligned = secs - secs.rem_euclid(window_secs);
        DateTime::from_timestamp(aligned, 0).unwrap_or(ts)
    }

    /// Инкрементально добавляет событие в соответствующее окно
    pub fn ingest(&mut self, event: &EventDto) {
        let key = AggregateKey {
            service: event.service.clone(),
            metric: event.metric.clone(),
            window_start: self.window_start_of(event.timestamp),
        };
        self.open_windows
            .entry(key)
            .and_modify(|agg| agg.add(event.value))
            .or_insert_with(|| AggregateState::new(event.value));
    }

    /// Закрывает все окна, чей конец <= now, и возвращает их как записи для БД.
    /// Возвращает Vec<(AggregateKey, AggregateState)>.
    pub fn close_windows(&mut self, now: DateTime<Utc>) -> Vec<(AggregateKey, AggregateState)> {
        let mut closed = Vec::new();
        let mut to_remove = Vec::new();

        for (key, _) in &self.open_windows {
            let window_end = key.window_start + self.window_size;
            if window_end <= now {
                to_remove.push(key.clone());
            }
        }

        for key in to_remove {
            if let Some(state) = self.open_windows.remove(&key) {
                closed.push((key, state));
            }
        }

        closed
    }

    /// Количество открытых окон (для тестов/метрик)
    pub fn open_window_count(&self) -> usize {
        self.open_windows.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_event(service: &str, metric: &str, value: f64, ts: DateTime<Utc>) -> EventDto {
        EventDto {
            id: uuid::Uuid::new_v4(),
            timestamp: ts,
            service: service.to_string(),
            metric: metric.to_string(),
            value,
            payload: None,
        }
    }

    #[test]
    fn test_window_alignment() {
        let agg = Aggregator::new(60);
        let ts = Utc.with_ymd_and_hms(2026, 10, 28, 14, 30, 45).unwrap();
        let window = agg.window_start_of(ts);
        assert_eq!(window, Utc.with_ymd_and_hms(2026, 10, 28, 14, 30, 0).unwrap());
    }

    #[test]
    fn test_aggregation_count_sum_min_max() {
        let mut agg = Aggregator::new(60);
        let base = Utc.with_ymd_and_hms(2026, 10, 28, 14, 30, 0).unwrap();

        agg.ingest(&make_event("auth", "login", 100.0, base + Duration::seconds(5)));
        agg.ingest(&make_event("auth", "login", 50.0, base + Duration::seconds(10)));
        agg.ingest(&make_event("auth", "login", 200.0, base + Duration::seconds(20)));

        let key = AggregateKey {
            service: "auth".to_string(),
            metric: "login".to_string(),
            window_start: base,
        };
        let state = agg.open_windows.get(&key).unwrap();
        assert_eq!(state.count, 3);
        assert_eq!(state.sum, 350.0);
        assert_eq!(state.min, 50.0);
        assert_eq!(state.max, 200.0);
    }

    #[test]
    fn test_close_windows() {
        let mut agg = Aggregator::new(60);
        let base = Utc.with_ymd_and_hms(2026, 10, 28, 14, 30, 0).unwrap();
        agg.ingest(&make_event("auth", "login", 100.0, base + Duration::seconds(5)));

        // Окно 14:30 закрывается только когда now >= 14:31
        let closed = agg.close_windows(base + Duration::seconds(30));
        assert!(closed.is_empty());
        assert_eq!(agg.open_window_count(), 1);

        let closed = agg.close_windows(base + Duration::seconds(61));
        assert_eq!(closed.len(), 1);
        assert_eq!(agg.open_window_count(), 0);
    }
}
