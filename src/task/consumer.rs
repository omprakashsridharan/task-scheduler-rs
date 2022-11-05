use super::{model::Task, repository::TaskRepository};
use crate::amqp::consumer::Consumer;

pub struct TaskConsumer<TR: TaskRepository> {
    consumer: Consumer,
    task_repository: TR,
}

impl<TR: TaskRepository> TaskConsumer<TR> {
    pub fn new(consumer: Consumer, task_repository: TR) -> Self {
        Self {
            consumer,
            task_repository,
        }
    }

    pub async fn consume(
        &self,
        task_type: String,
    ) -> Result<Option<Task>, Box<dyn std::error::Error>> {
        let task = self.consumer.consume::<Task>(task_type).await?;
        let is_task_valid = self
            .task_repository
            .is_task_valid(task.task_id.clone())
            .await?;
        if is_task_valid {
            return Ok(Some(task));
        } else {
            return Ok(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use testcontainers::{
        clients,
        images::{rabbitmq, redis::Redis},
    };

    use super::TaskConsumer;
    use crate::{
        amqp::{base::Amqp, consumer::Consumer, producer::Producer},
        redis_helper::RedisHelper,
        task::{model::Task, repository::TaskRepositoryImpl, scheduler::SchedulerImpl},
    };
    use tokio_test::task::{self as tokio_test_task};

    #[tokio::test]
    async fn consuming_task() {
        let docker = clients::Cli::default();
        let rabbit_node = docker.run(rabbitmq::RabbitMq::default());
        let rabbit_mq_url = format!("amqp://127.0.0.1:{}", rabbit_node.get_host_port_ipv4(5672));
        let amqp = Amqp::new(rabbit_mq_url).await.unwrap();

        let amqp_producer = Producer::new(amqp.clone()).await.unwrap();
        let amqp_consumer = Consumer::new(amqp.clone()).await.unwrap();

        let redis_node = docker.run(Redis::default());
        let host_port = redis_node.get_host_port_ipv4(6379);
        let redis_helper = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port)).unwrap();
        let task_repository = TaskRepositoryImpl::new(redis_helper.clone());
        let task_consumer = TaskConsumer::new(amqp_consumer, task_repository.clone());
        let scheduler = SchedulerImpl::new(amqp_producer, task_repository);

        tokio_test_task::spawn(async {
            let task = Task::new("TASK1".to_string(), 3, "MESSAGE".to_string());
            let _task_id = scheduler.schedule_task(2000, task.clone()).await.unwrap();

            tokio::time::sleep(Duration::from_millis((2000 + 10) as u64)).await;
            let result_task = task_consumer.consume("TASK1".to_string()).await.unwrap();
            assert_eq!(result_task, Some(task));
        })
        .await;

        tokio_test_task::spawn(async {
            let task = Task::new("TASK2".to_string(), 1, "MESSAGE".to_string());
            let _task_id = scheduler.schedule_task(2000, task.clone()).await.unwrap();

            tokio::time::sleep(Duration::from_millis((2000 + 10) as u64)).await;
            let result_task = task_consumer.consume("TASK1".to_string()).await.unwrap();
            assert_eq!(result_task, None);
        })
        .await;
    }
}
