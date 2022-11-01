mod redis;
mod settings;
mod task_repository;

use crate::settings::{Settings, SETTINGS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings::init().unwrap();
    SETTINGS.set(settings).unwrap();

    let redis_url = Settings::global().redis_url.clone();
    Ok(())
}
