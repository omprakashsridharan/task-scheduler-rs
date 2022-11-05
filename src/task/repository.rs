use crate::redis_helper::RedisHelper;

#[async_trait::async_trait]
pub trait TaskRepository: Clone + Send + Sync {
    async fn create_task(
        &self,
        task_id: String,
        ttl_in_seconds: usize,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn delete_task(&self, task_id: String) -> Result<(), Box<dyn std::error::Error>>;
    async fn is_task_valid(&self, task_id: String) -> Result<bool, Box<dyn std::error::Error>>;
}

#[derive(Clone)]
pub struct TaskRepositoryImpl {
    redis_helper: RedisHelper,
}

impl TaskRepositoryImpl {
    pub fn new(redis_helper: RedisHelper) -> Self {
        Self { redis_helper }
    }
}

#[async_trait::async_trait]
impl TaskRepository for TaskRepositoryImpl {
    async fn create_task(
        &self,
        task_id: String,
        ttl_in_seconds: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.redis_helper
            .set(task_id, "".to_string(), Some(ttl_in_seconds))
            .await?;
        Ok(())
    }
    async fn delete_task(&self, task_id: String) -> Result<(), Box<dyn std::error::Error>> {
        self.redis_helper.delete(task_id).await?;
        Ok(())
    }
    async fn is_task_valid(&self, task_id: String) -> Result<bool, Box<dyn std::error::Error>> {
        let task_exists = self.redis_helper.exists(task_id).await?;
        Ok(task_exists)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use testcontainers::{clients, images::redis::Redis, Container};

    use crate::redis_helper::RedisHelper;

    use super::{TaskRepository, TaskRepositoryImpl};

    #[tokio::test]
    async fn creates_task_and_removes_after_expiry() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port)).unwrap();
        let task_repository = TaskRepositoryImpl::new(redis_helper);
        let task_id = "TASK1".to_string();
        task_repository
            .create_task(task_id.clone(), 2)
            .await
            .unwrap();
        let is_task_valid = task_repository
            .is_task_valid(task_id.clone())
            .await
            .unwrap();
        assert!(is_task_valid);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let is_task_valid = task_repository
            .is_task_valid(task_id.clone())
            .await
            .unwrap();
        assert!(!is_task_valid);
    }

    #[tokio::test]
    async fn deletes_task_immediately() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port)).unwrap();
        let task_repository = TaskRepositoryImpl::new(redis_helper);
        let task_id = "TASK1".to_string();
        task_repository
            .create_task(task_id.clone(), 5)
            .await
            .unwrap();
        let is_task_valid = task_repository
            .is_task_valid(task_id.clone())
            .await
            .unwrap();
        assert!(is_task_valid);

        task_repository.delete_task(task_id.clone()).await.unwrap();
        let is_task_valid = task_repository
            .is_task_valid(task_id.clone())
            .await
            .unwrap();
        assert!(!is_task_valid);
    }
}
