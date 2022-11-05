use std::time::Duration;

use borsh::BorshDeserialize;
use futures_lite::stream::StreamExt;
use lapin::{
    options::{BasicConsumeOptions, ExchangeDeclareOptions, QueueDeclareOptions},
    topology::{BindingDefinition, ExchangeDefinition, QueueDefinition},
    types::{FieldTable, ShortString},
    Error,
};

use super::base::Amqp;

#[derive(Clone)]
pub struct Consumer {
    amqp: Amqp,
}

impl Consumer {
    pub async fn new(amqp: Amqp) -> Result<Self, Error> {
        Ok(Self { amqp })
    }

    pub async fn consume<T: BorshDeserialize>(&self, task_type: String) -> Result<T, Error> {
        let queue_name = task_type.clone();
        let final_queue = queue_name.clone();
        let final_exchange = format!("{}_final_exchange", queue_name.clone());

        let channel = self
            .amqp
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
            .await?;
        let mut consumer = channel
            .basic_consume(
                &task_type,
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let consumed = tokio::time::timeout(Duration::from_millis(2500), consumer.next())
            .await
            .unwrap()
            .unwrap();
        let delivery = consumed?;
        let result = T::try_from_slice(&delivery.data)?;
        Ok(result)
    }
}
