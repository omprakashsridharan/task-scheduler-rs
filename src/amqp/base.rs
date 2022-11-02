use std::sync::Arc;

use lapin::{Connection, ConnectionProperties, Error};

pub struct Amqp {
    connection: Arc<Connection>,
}

impl Amqp {
    pub async fn new(rabbit_mq_url: String) -> Result<Self, Error> {
        let options = ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);
        let connection = Connection::connect(&rabbit_mq_url, options).await?;
        Ok(Amqp {
            connection: Arc::new(connection),
        })
    }
}

#[cfg(test)]
mod amqp_base_tests {
    use testcontainers::{clients, images::rabbitmq};

    use super::*;

    #[tokio::test]
    async fn results_in_ok_for_valid_rabbit_mq_error() {
        let docker = clients::Cli::default();
        let rabbit_node = docker.run(rabbitmq::RabbitMq::default());
        let rabbit_mq_url = format!("amqp://127.0.0.1:{}", rabbit_node.get_host_port_ipv4(5672));
        let connection_result = Amqp::new(rabbit_mq_url).await;
        assert!(connection_result.is_ok());
    }
}
