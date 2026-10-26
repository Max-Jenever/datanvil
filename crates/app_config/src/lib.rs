use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub database: DatabaseSettings,
    pub redis: RedisSettings,
    pub kafka: KafkaSettings,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseSettings {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedisSettings {
    pub url: String,
    pub pool_size: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaSettings {
    pub brokers: String,
    pub consumer_group: String,
    pub topic_events: String,
}

pub fn get_settings() -> Result<Settings, config::ConfigError> {
    let builder = config::Config::builder().add_source(
        config::Environment::default()
            .separator("__")
            .try_parsing(true),
    );

    builder.build()?.try_deserialize()
}
