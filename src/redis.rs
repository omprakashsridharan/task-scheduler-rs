use redis::{AsyncCommands, Client, RedisError, RedisResult};

struct RedisHelper {
    client: Client,
}

impl RedisHelper {
    pub fn new(redis_url: String) -> RedisResult<Self> {
        let client = Client::open(redis_url)?;
        Ok(Self { client })
    }

    pub async fn set(&self, key: String, value: String) -> Result<(), RedisError> {
        let mut connection = self.client.get_async_connection().await?;
        connection.set(key, value).await?;
        Ok(())
    }

    pub async fn get(&self, key: String) -> Result<String, RedisError> {
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

    pub async fn expire(&self, key: String, seconds: usize) -> Result<(), RedisError> {
        let mut connection = self.client.get_async_connection().await?;
        connection.expire(key, seconds).await?;
        Ok(())
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
        assert_eq!(redis_helper_result.is_err(), true);
    }

    #[test]
    fn returns_valid_redis_connection_for_valid_url() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert_eq!(redis_helper_result.is_ok(), true);
    }

    #[tokio::test]
    async fn set_get_key_works() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert_eq!(redis_helper_result.is_ok(), true);
        if let Ok(redis_helper) = redis_helper_result {
            let set_result = redis_helper
                .set("KEY".to_string(), "VALUE".to_string())
                .await;
            assert_eq!(set_result.is_ok(), true);
            let get_result = redis_helper.get("KEY".to_string()).await;
            assert_eq!(get_result, Ok("VALUE".to_string()));
        }
    }

    #[tokio::test]
    async fn delete_key_works() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert_eq!(redis_helper_result.is_ok(), true);
        if let Ok(redis_helper) = redis_helper_result {
            let set_result = redis_helper
                .set("KEY".to_string(), "VALUE".to_string())
                .await;
            assert_eq!(set_result.is_ok(), true);
            let delete_result = redis_helper.delete("KEY".to_string()).await;
            assert_eq!(delete_result.is_ok(), true);
        }
    }

    #[tokio::test]
    async fn returns_true_for_existing_key() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_helper_result = RedisHelper::new(format!("redis://127.0.0.1:{}", host_port));
        assert_eq!(redis_helper_result.is_ok(), true);
        if let Ok(redis_helper) = redis_helper_result {
            let set_result = redis_helper
                .set("KEY".to_string(), "VALUE".to_string())
                .await;
            assert_eq!(set_result.is_ok(), true);
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
        assert_eq!(redis_helper_result.is_ok(), true);
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
        assert_eq!(redis_helper_result.is_ok(), true);
        if let Ok(redis_helper) = redis_helper_result {
            let set_result = redis_helper
                .set("KEY".to_string(), "VALUE".to_string())
                .await;
            assert_eq!(set_result.is_ok(), true);
            const TIME_TO_SLEEP: u64 = 2;
            redis_helper
                .expire("KEY".to_string(), TIME_TO_SLEEP as usize)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(TIME_TO_SLEEP)).await;
            let exist_result = redis_helper.exists("KEY".to_string()).await;
            assert_eq!(exist_result, Ok(false));
        }
    }
}
