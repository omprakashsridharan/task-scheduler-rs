use borsh::{BorshDeserialize, BorshSerialize};
use uuid::Uuid;

use crate::{amqp::producer::Producer, task_repository::TaskRepository};

#[derive(BorshDeserialize, BorshSerialize)]
pub struct Task {
    pub task_id: String,
    pub task_type: String,
    pub time_to_live_in_seconds: usize,
    pub payload: String,
}

impl Task {
    pub fn new(task_type: String, time_to_live_in_seconds: usize, payload: String) -> Self {
        Self {
            task_id: Uuid::new_v4().to_string(),
            task_type,
            time_to_live_in_seconds,
            payload,
        }
    }
}

#[async_trait::async_trait]
pub trait Scheduler {
    async fn schedule_task(
        &self,
        delay_in_milliseconds: usize,
        task: Task,
    ) -> Result<String, Box<dyn std::error::Error>>;
    async fn invalidate_task(&self, task_id: String) -> Result<(), Box<dyn std::error::Error>>;
}

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
}

#[async_trait::async_trait]
impl<TR> Scheduler for SchedulerImpl<TR>
where
    TR: TaskRepository,
{
    async fn schedule_task(
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
    async fn invalidate_task(&self, task_id: String) -> Result<(), Box<dyn std::error::Error>> {
        self.task_repository.delete_task(task_id).await?;
        Ok(())
    }
}
