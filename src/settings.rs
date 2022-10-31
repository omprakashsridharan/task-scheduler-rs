use config::{Config, ConfigError, Environment};
use serde::Deserialize;
use once_cell::sync::OnceCell;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub port: u16,
    pub rabbit_mq_url: String,
    pub redis_url: String,
    pub app_name: String,
}

impl Settings {
    pub fn global() -> &'static Settings {
        SETTINGS.get().expect("settings is not initialized")
    }

    pub fn init() -> Result<Settings, ConfigError> {
        dotenv::dotenv().ok();
        let s = Config::builder()
            .add_source(Environment::default())
            .set_default("port",3000)?
            .set_default("app_name","Distributed Task scheduler using RabbitMQ")?
            .build()?;
        s.try_deserialize()
    }
}



pub static SETTINGS: OnceCell<Settings> = OnceCell::new();