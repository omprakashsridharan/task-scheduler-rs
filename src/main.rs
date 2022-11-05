mod amqp;
mod redis_helper;
mod settings;
mod task;

use std::convert::Infallible;

use amqp::{base::Amqp, consumer::Consumer, producer::Producer};
use redis_helper::RedisHelper;
use task::{
    consumer::TaskConsumer,
    model::{ConsumeRequest, Task, TaskExpired, TaskRequest},
    scheduler::SchedulerImpl,
};
use warp::{hyper::StatusCode, reject, Filter, Rejection};

use crate::{
    settings::{Settings, SETTINGS},
    task::repository::TaskRepositoryImpl,
};

fn with_task_request_json_body(
) -> impl Filter<Extract = (TaskRequest,), Error = warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

fn with_consume_request_json_body(
) -> impl Filter<Extract = (ConsumeRequest,), Error = warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

fn with_task_scheduler(
    task_scheduler: SchedulerImpl<TaskRepositoryImpl>,
) -> impl Filter<Extract = (SchedulerImpl<TaskRepositoryImpl>,), Error = std::convert::Infallible> + Clone
{
    warp::any().map(move || task_scheduler.clone())
}

fn with_task_consumer(
    task_consumer: TaskConsumer<TaskRepositoryImpl>,
) -> impl Filter<Extract = (TaskConsumer<TaskRepositoryImpl>,), Error = std::convert::Infallible> + Clone
{
    warp::any().map(move || task_consumer.clone())
}

pub async fn schedule(
    tr: TaskRequest,
    task_scheduler: SchedulerImpl<TaskRepositoryImpl>,
) -> Result<impl warp::Reply, Infallible> {
    let task = Task::new(tr.task_type, tr.time_to_live_in_seconds, tr.payload);
    task_scheduler
        .schedule_task(tr.time_to_live_in_seconds, task)
        .await
        .unwrap();
    Ok(StatusCode::CREATED)
}

pub async fn consume(
    cr: ConsumeRequest,
    task_consumer: TaskConsumer<TaskRepositoryImpl>,
) -> Result<impl warp::Reply, Rejection> {
    let task = task_consumer.consume(cr.task_type).await.unwrap();
    if task.is_some() {
        let result = task.unwrap();
        Ok(warp::reply::json(&result))
    } else {
        Err(reject::custom(TaskExpired))
    }
}

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

    let schedule = warp::path!("schedule")
        .and(warp::post())
        .and(with_task_request_json_body())
        .and(with_task_scheduler(task_scheduler))
        .and_then(schedule);

    let consume = warp::path!("consume")
        .and(warp::post())
        .and(with_consume_request_json_body())
        .and(with_task_consumer(task_consumer))
        .and_then(consume);

    warp::serve(schedule.or(consume))
        .run(([127, 0, 0, 1], Settings::global().port))
        .await;

    Ok(())
}
