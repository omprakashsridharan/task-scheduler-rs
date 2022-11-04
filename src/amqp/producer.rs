use std::collections::BTreeMap;

use borsh::BorshSerialize;
use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions, QueueDeclareOptions},
    protocol::basic::AMQPProperties,
    topology::{BindingDefinition, ExchangeDefinition, QueueDefinition},
    types::AMQPValue,
    types::{FieldTable, LongString, ShortString},
    Error,
};

use super::base::Amqp;

pub struct Producer {
    amqp: Amqp,
}

impl Producer {
    pub async fn new(amqp: Amqp) -> Result<Self, Error> {
        Ok(Self { amqp })
    }

    pub async fn send_delayed_message_to_queue<T: BorshSerialize>(
        &self,
        task_type: String,
        delay_in_milli_seconds: usize,
        data: T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let queue_name = task_type;
        let intermediate_queue: String = format!("{}_intermediate_queue", queue_name.clone());
        let intermediate_exchange = format!("{}_intermediate_exchange", queue_name.clone());

        let final_queue = queue_name.clone();
        let final_exchange = format!("{}_final_exchange", queue_name.clone());

        let channel = self
            .amqp
            .get_channel(
                vec![
                    ExchangeDefinition {
                        name: ShortString::from(intermediate_exchange.clone()),
                        kind: Some(lapin::ExchangeKind::Fanout),
                        options: Some(ExchangeDeclareOptions::default()),
                        arguments: Some(FieldTable::default()),
                        bindings: vec![],
                    },
                    ExchangeDefinition {
                        name: ShortString::from(final_exchange.clone()),
                        kind: Some(lapin::ExchangeKind::Fanout),
                        options: Some(ExchangeDeclareOptions::default()),
                        arguments: Some(FieldTable::default()),
                        bindings: vec![],
                    },
                ],
                vec![
                    QueueDefinition {
                        name: ShortString::from(intermediate_queue.clone()),
                        options: Some(QueueDeclareOptions::default()),
                        arguments: Some(
                            BTreeMap::from([(
                                ShortString::from("x-dead-letter-exchange"),
                                AMQPValue::LongString(LongString::from(final_exchange.clone())),
                            )])
                            .into(),
                        ),
                        bindings: vec![BindingDefinition {
                            source: ShortString::from(intermediate_exchange.clone()),
                            routing_key: ShortString::from(""),
                            arguments: FieldTable::default(),
                        }],
                    },
                    QueueDefinition {
                        name: ShortString::from(final_queue.clone()),
                        options: Some(QueueDeclareOptions::default()),
                        arguments: Some(FieldTable::default()),
                        bindings: vec![BindingDefinition {
                            source: ShortString::from(final_exchange.clone()),
                            routing_key: ShortString::from(""),
                            arguments: FieldTable::default(),
                        }],
                    },
                ],
            )
            .await?;
        let publisher_properties = AMQPProperties::default()
            .with_expiration(ShortString::from(delay_in_milli_seconds.to_string()));
        let mut buffer = Vec::new();
        data.serialize(&mut buffer)?;
        channel
            .basic_publish(
                &intermediate_exchange,
                "",
                BasicPublishOptions::default(),
                &buffer,
                publisher_properties,
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use borsh::BorshDeserialize;
    use futures_lite::stream::StreamExt;
    use lapin::options::BasicConsumeOptions;
    use testcontainers::{clients, images::rabbitmq};

    use super::*;

    #[tokio::test]
    async fn test_send_message_delay() {
        let docker = clients::Cli::default();
        let rabbit_node = docker.run(rabbitmq::RabbitMq::default());
        let rabbit_mq_url = format!("amqp://127.0.0.1:{}", rabbit_node.get_host_port_ipv4(5672));
        let amqp = Amqp::new(rabbit_mq_url).await.unwrap();
        let task_type = "TASK_TYPE1".to_string();

        let producer = Producer::new(amqp.clone()).await.unwrap();
        producer
            .send_delayed_message_to_queue(task_type.clone(), 2000, "MESSAGE".to_string())
            .await
            .unwrap();

        let channel = amqp.get_channel(vec![], vec![]).await.unwrap();

        let mut consumer = channel
            .basic_consume(
                &task_type.as_str(),
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        let consumed = tokio::time::timeout(Duration::from_millis(2500), consumer.next())
            .await
            .unwrap()
            .unwrap();
        let delivery = consumed.expect("Failed to consume delivery!");
        let message = String::try_from_slice(&delivery.data.clone()).unwrap();
        assert_eq!(message, "MESSAGE".to_string());
        assert_eq!(
            delivery.exchange.as_str(),
            format!("{}_final_exchange", task_type.clone())
        );
    }
}
