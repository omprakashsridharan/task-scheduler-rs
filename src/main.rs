mod amqp;
mod redis_helper;
mod settings;
mod task;

use amqp::{base::Amqp, consumer::Consumer, producer::Producer};
use redis_helper::RedisHelper;
use task::{consumer::TaskConsumer, scheduler::SchedulerImpl};

use crate::{
    settings::{Settings, SETTINGS},
    task::repository::TaskRepositoryImpl,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings::init().unwrap();
    SETTINGS.set(settings).unwrap();

    let redis_url = Settings::global().redis_url.clone();
    let redis_helper = RedisHelper::new(redis_url)?;

    let rabbit_mq_url = Settings::global().rabbit_mq_url.clone();
    let amqp = Amqp::new(rabbit_mq_url).await?;
    let amqp_producer = Producer::new(amqp.clone()).await?;
    let amqp_consumer = Consumer::new(amqp).await?;

    let task_repository = TaskRepositoryImpl::new(redis_helper);
    let task_scheduler = SchedulerImpl::new(amqp_producer, task_repository.clone());
    let task_consumer = TaskConsumer::new(amqp_consumer, task_repository);
    Ok(())
}
