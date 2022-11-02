use std::collections::BTreeMap;

use lapin::{
    options::{
        BasicPublishOptions, ExchangeBindOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
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

    pub async fn send_delayed_message_to_queue(
        &self,
        task_type: String,
        delay_in_milli_seconds: usize,
        data: String,
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
        channel
            .basic_publish(
                &intermediate_exchange,
                "",
                BasicPublishOptions::default(),
                data.as_bytes(),
                publisher_properties,
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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

        let queue_name = task_type.clone();

        let final_queue = queue_name.clone();
        let final_exchange = format!("{}_final_exchange", queue_name.clone());

        let channel = amqp
            .get_channel(
                vec![ExchangeDefinition {
                    name: ShortString::from(final_exchange.clone()),
                    kind: Some(lapin::ExchangeKind::Fanout),
                    options: Some(ExchangeDeclareOptions::default()),
                    arguments: Some(FieldTable::default()),
                    bindings: vec![],
                }],
                vec![QueueDefinition {
                    name: ShortString::from(final_queue.clone()),
                    options: Some(QueueDeclareOptions::default()),
                    arguments: Some(FieldTable::default()),
                    bindings: vec![BindingDefinition {
                        source: ShortString::from(final_exchange.clone()),
                        routing_key: ShortString::from(""),
                        arguments: FieldTable::default(),
                    }],
                }],
            )
            .await
            .unwrap();

        let mut consumer = channel
            .basic_consume(
                final_queue.as_str(),
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        let producer = Producer::new(amqp.clone()).await.unwrap();
        producer
            .send_delayed_message_to_queue(task_type.clone(), 2000, "MESSAGE".to_string())
            .await
            .unwrap();

        let consumed = tokio::time::timeout(Duration::from_millis(2500), consumer.next())
            .await
            .unwrap()
            .unwrap();
        let delivery = consumed.expect("Failed to consume delivery!");

        assert_eq!(String::from_utf8(delivery.data.clone()).unwrap(), "MESSAGE");
        assert_eq!(
            delivery.exchange.as_str(),
            format!("{}_final_exchange", task_type.clone())
        );
    }
}
