use redis::{AsyncCommands, Client, RedisError, RedisResult};

#[derive(Clone)]
pub struct RedisHelper {
    client: Client,
}

impl RedisHelper {
    pub fn new(redis_url: String) -> RedisResult<Self> {
        let client = Client::open(redis_url)?;
        Ok(Self { client })
    }

    pub async fn set(
        &self,
        key: String,
        value: String,
        ttl_in_seconds: Option<usize>,
    ) -> Result<(), RedisError> {
        let mut connection = self.client.get_async_connection().await?;
        if let Some(ttl) = ttl_in_seconds {
            connection.set_ex(key, value, ttl).await?;
        } else {
            connection.set(key, value).await?;
        }
        Ok(())
    }

    pub async fn _get(&self, key: String) -> Result<String, RedisError> {
        let mut connection = self.client.get_async_connection().await?;
        let value: String = connection.get(key).await?;
        Ok(value)
    }

    pub async fn delete(&self, key: String) -> Result<(), RedisError> {
        let mut connection = self.client.get_async_connection().await?;
        connection.del(key).await?;
        Ok(())
    }

    pub async fn exists(&self, key: String) -> Result<bool, RedisError> {
        let mut connection = self.client.get_async_connection().await?;
        let result = connection.exists(key).await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RedisHelper;
    use testcontainers::{clients, images::redis::Redis, Container};

    #[test]
    fn returns_error_result_for_invalid_redis_connection_url() {
        let redis_helper_result = RedisHelper::new("INVALID".to_string());
        assert!(redis_helper_result.is_err());
    }

    #[test]
    fn returns_valid_redis_connection_for_valid_url() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert!(redis_helper_result.is_ok());
    }

    #[tokio::test]
    async fn set_get_key_works() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert!(redis_helper_result.is_ok());
        if let Ok(redis_helper) = redis_helper_result {
            let set_result = redis_helper
                .set("KEY".to_string(), "VALUE".to_string(), None)
                .await;
            assert!(set_result.is_ok());
            let get_result = redis_helper._get("KEY".to_string()).await;
            assert_eq!(get_result, Ok("VALUE".to_string()));
        }
    }

    #[tokio::test]
    async fn delete_key_works() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert!(redis_helper_result.is_ok());
        if let Ok(redis_helper) = redis_helper_result {
            let set_result = redis_helper
                .set("KEY".to_string(), "VALUE".to_string(), None)
                .await;
            assert!(set_result.is_ok());
            let delete_result = redis_helper.delete("KEY".to_string()).await;
            assert!(delete_result.is_ok());
        }
    }

    #[tokio::test]
    async fn returns_true_for_existing_key() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert!(redis_helper_result.is_ok());
        if let Ok(redis_helper) = redis_helper_result {
            let set_result = redis_helper
                .set("KEY".to_string(), "VALUE".to_string(), None)
                .await;
            assert!(set_result.is_ok());
            let delete_result = redis_helper.exists("KEY".to_string()).await;
            assert_eq!(delete_result, Ok(true));
        }
    }

    #[tokio::test]
    async fn returns_true_for_non_existing_key() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert!(redis_helper_result.is_ok());
        if let Ok(redis_helper) = redis_helper_result {
            let delete_result = redis_helper.exists("INVALID_KEY".to_string()).await;
            assert_eq!(delete_result, Ok(false));
        }
    }

    #[tokio::test]
    async fn key_expires_after_specified_seconds() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert!(redis_helper_result.is_ok());
        if let Ok(redis_helper) = redis_helper_result {
            const TIME_TO_SLEEP: u64 = 2;
            let set_result = redis_helper
                .set(
                    "KEY".to_string(),
                    "VALUE".to_string(),
                    Some(TIME_TO_SLEEP as usize),
                )
                .await;
            assert!(set_result.is_ok());
            tokio::time::sleep(Duration::from_secs(TIME_TO_SLEEP)).await;
            let exist_result = redis_helper.exists("KEY".to_string()).await;
            assert_eq!(exist_result, Ok(false));
        }
    }
}
