use redis::{AsyncCommands, Client, RedisError, RedisResult};
struct RedisHelper {
    client: Client,
}

impl RedisHelper {
    pub fn new(redis_url: String) -> RedisResult<Self> {
        let client = redis::Client::open(redis_url)?;
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
}

#[cfg(test)]
mod tests {
    use crate::redis::RedisHelper;
    use testcontainers::{clients, images::redis::Redis, Container};

    #[test]
    fn redis_connection_invalid_throws_error() {
        let redis_helper_result = RedisHelper::new("INVALID".to_string());
        assert_eq!(redis_helper_result.is_err(), true)
    }

    #[test]
    fn redis_connection_valid() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_url = format!("redis://127.0.0.1:{}", host_port);
        let redis_helper_result = RedisHelper::new(redis_url);
        assert_eq!(redis_helper_result.is_ok(), true);
    }

    #[tokio::test]
    async fn set_get_works() {
        let docker = clients::Cli::default();
        let node: Container<Redis> = docker.run(Redis::default());
        let host_port = node.get_host_port_ipv4(6379);
        let redis_url = format!("redis://127.0.0.1:{}", host_port);
        let redis_helper_result = RedisHelper::new(redis_url);
        assert_eq!(redis_helper_result.is_ok(), true);
        if let Ok(redis_helper) = redis_helper_result {
            let set_result = redis_helper
                .set("KEY".to_string(), "VALUE".to_string())
                .await;
            assert_eq!(set_result.is_ok(), true);
            let get_result = redis_helper.get("KEY".to_string()).await;
            assert_eq!(get_result, Ok("VALUE".to_string()))
        }
    }
}
