use redis::{Client, RedisResult};
struct RedisHelper {
    client: Client,
}

impl RedisHelper {
    pub fn new(redis_url: String) -> RedisResult<Self> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self { client })
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
}
