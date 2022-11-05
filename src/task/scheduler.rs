use super::{model::Task, repository::TaskRepository};
use crate::amqp::producer::Producer;

#[derive(Clone)]
pub struct SchedulerImpl<TR: TaskRepository> {
    producer: Producer,
    task_repository: TR,
}

impl<TR> SchedulerImpl<TR>
where
    TR: TaskRepository,
{
    pub fn new(producer: Producer, task_repository: TR) -> Self {
        Self {
            producer,
            task_repository,
        }
    }

    pub async fn schedule_task(
        &self,
        delay_in_milliseconds: usize,
        task: Task,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let task_id = task.task_id.clone();
        let task_type = task.task_type.clone();
        self.task_repository
            .create_task(task_id.clone(), task.time_to_live_in_seconds)
            .await?;
        self.producer
            .send_delayed_message_to_queue(task_type, delay_in_milliseconds, task)
            .await?;
        Ok(task_id)
    }

    pub async fn invalidate_task(&self, task_id: String) -> Result<(), Box<dyn std::error::Error>> {
        self.task_repository.delete_task(task_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use testcontainers::{
        clients,
        images::{rabbitmq, redis::Redis},
    };

    use super::*;
    use crate::{
        amqp::{base::Amqp, consumer::Consumer, producer::Producer},
        redis_helper::RedisHelper,
        task::repository::TaskRepositoryImpl,
    };
    use tokio_test::task::{self as tokio_test_task};

    #[tokio::test]
    async fn test_schedule_task() {
        let docker = clients::Cli::default();
        let rabbit_node = docker.run(rabbitmq::RabbitMq::default());
        let rabbit_mq_url = format!("amqp://127.0.0.1:{}", rabbit_node.get_host_port_ipv4(5672));
        let amqp = Amqp::new(rabbit_mq_url).await.unwrap();

        let producer = Producer::new(amqp.clone()).await.unwrap();

        let redis_node = docker.run(Redis::default());
        let host_port = redis_node.get_host_port_ipv4(6379);
        let redis_helper = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port)).unwrap();
        let task_repository = TaskRepositoryImpl::new(redis_helper.clone());

        let scheduler = SchedulerImpl::new(producer, task_repository);
        let task_type = "SAMPLE".to_string();
        let delay_in_milliseconds = 2000;
        let time_to_live_in_seconds = 1;
        let todo_task = Task::new(task_type.clone(), time_to_live_in_seconds, "test".into());
        let task_id = scheduler
            .schedule_task(delay_in_milliseconds, todo_task.clone())
            .await
            .unwrap();

        tokio_test_task::spawn(async {
            let exist_result = redis_helper.clone().exists(task_id.clone()).await;
            assert_eq!(exist_result, Ok(true));
            tokio::time::sleep(Duration::from_secs(time_to_live_in_seconds as u64)).await;

            let exist_result = redis_helper.exists(task_id.clone()).await;
            assert_eq!(exist_result, Ok(false));
        })
        .await;

        tokio_test_task::spawn(async {
            let consumer = Consumer::new(amqp.clone()).await.unwrap();
            let obtained_task = consumer.consume::<Task>(task_type).await.unwrap();
            assert_eq!(obtained_task, todo_task);
        })
        .await;
    }
}
