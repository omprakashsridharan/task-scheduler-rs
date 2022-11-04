use std::error::Error;

use crate::amqp::consumer::Consumer;

use super::{model::Task, repository::TaskRepository};

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

    pub async fn consumer(
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
