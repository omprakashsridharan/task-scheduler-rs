use std::sync::Arc;

use lapin::{
    options::QueueBindOptions,
    topology::{ExchangeDefinition, QueueDefinition},
    Channel, Connection, ConnectionProperties, Error,
};

#[derive(Clone)]
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

    pub async fn get_channel(
        &self,
        exchange_definitions: Vec<ExchangeDefinition>,
        queue_definitions: Vec<QueueDefinition>,
    ) -> Result<Channel, Error> {
        let channel = self.connection.create_channel().await?;
        for ed in exchange_definitions.iter() {
            let exchange = ed.name.as_str();
            let kind = ed.kind.as_ref().unwrap().to_owned();
            let options = ed.options.as_ref().unwrap().to_owned();
            let argumemts = ed.arguments.as_ref().unwrap().to_owned();
            channel
                .exchange_declare(exchange, kind, options, argumemts)
                .await?;
        }

        for qd in queue_definitions.iter() {
            let queue = qd.name.as_str();
            let options = qd.options.as_ref().unwrap().to_owned();
            let argumemts = qd.arguments.as_ref().unwrap().to_owned();
            channel.queue_declare(queue, options, argumemts).await?;
            for b in qd.bindings.iter() {
                channel
                    .queue_bind(
                        queue,
                        b.source.as_str(),
                        "",
                        QueueBindOptions::default(),
                        b.arguments.to_owned(),
                    )
                    .await?;
            }
        }

        Ok(channel)
    }
}

#[cfg(test)]
mod tests {
    use testcontainers::{clients, images::rabbitmq};

    use super::*;

    #[tokio::test]
    async fn results_in_ok_for_valid_rabbit_mq_error() {
        let docker = clients::Cli::default();
        let rabbit_node = docker.run(rabbitmq::RabbitMq::default());
        let rabbit_mq_url = format!("amqp://127.0.0.1:{}", rabbit_node.get_host_port_ipv4(5672));
        let amp = Amqp::new(rabbit_mq_url).await;
        assert!(amp.is_ok());
    }

    #[tokio::test]
    async fn results_in_error_for_invalid_rabbit_mq_error() {
        let rabbit_mq_url = "INVALID".to_string();
        let amp = Amqp::new(rabbit_mq_url).await;
        assert!(amp.is_err());
    }

    #[tokio::test]
    async fn creates_channel_successfully() {
        let docker = clients::Cli::default();
        let rabbit_node = docker.run(rabbitmq::RabbitMq::default());
        let rabbit_mq_url = format!("amqp://127.0.0.1:{}", rabbit_node.get_host_port_ipv4(5672));
        let amqp = Amqp::new(rabbit_mq_url).await.unwrap();
        let channel_result = amqp.get_channel(vec![], vec![]).await;
        assert!(channel_result.is_ok());
        assert!(channel_result.ok().unwrap().status().connected());
    }
}
