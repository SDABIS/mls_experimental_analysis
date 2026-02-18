use std::{thread};
use futures::executor::block_on;
//use libp2p::{gossipsub, mdns, noise, tcp, yamux};
use rumqttc::{AsyncClient, QoS};
use tokio::sync::mpsc::{Sender};
use crate::pubsub::mqtt_updater::MQTTQueueMessage;
use ds_lib::*;
use openmls::prelude::*;
use crate::pubsub::Broker;
use tls_codec::Serialize;

#[derive(Debug)]
pub struct MqttBroker {
    username: String,
    async_client: AsyncClient,
    tx: Sender<MQTTQueueMessage>,
}

impl Broker for MqttBroker {
    fn subscribe(&mut self, group_name: String) -> Result<(), String> {
        let topic = format!("cgka/group/{group_name}");
        let tx = self.tx.clone();
        let name = self.username.clone();
        let topic_thread = topic.clone();

        thread::spawn(move || {
            //let (sender, receiver) = tokio::sync::oneshot::channel::<()>();

            tx.blocking_send(MQTTQueueMessage::Subscribe(name, topic_thread))
                .map_err(|e| format!("Error sending to queue: {:?}", e)).unwrap();

            //receiver.blocking_recv()
            //    .map_err(|e| format!("Error receiving from channel: {:?}", e)).unwrap();
        }).join().expect("Thread panicked");

        Ok(())
   }

    fn unsubscribe(&mut self, group_name: String) -> Result<(), String> {
        let topic = format!("cgka/group/{group_name}");
        let tx = self.tx.clone();
        let name = self.username.clone();
        let topic_thread = topic.clone();

        thread::spawn(move || {
            
            tx.blocking_send(MQTTQueueMessage::Unsubscribe(name, topic_thread))
                .map_err(|e| format!("Error sending to queue: {:?}", e)).unwrap();
        }).join().expect("Thread panicked");

        Ok(())
    }

    fn subscribe_welcome(&mut self, user_name: String) -> Result<(), String> {

        let topic = format!("cgka/welcome/{user_name}");
        let tx = self.tx.clone();
        let name = self.username.clone();
        let topic_thread = topic.clone();

        thread::spawn(move || {
            //let (sender, _receiver) = tokio::sync::oneshot::channel::<()>();

            tx.blocking_send(MQTTQueueMessage::Subscribe(name, topic_thread))
                .map_err(|e| format!("Error sending to queue: {:?}", e.0)).unwrap();

        }).join().expect("Thread panicked");

        Ok(())
    }

    fn send_ack(&self, ack: &WelcomeAcknowledgement) -> Result<(), String> {
        let group_name = ack.group.clone();

        let ds_message = DSMessage::WelcomeAcknowledgement(ack.clone());
        let serialized_msg = ds_message.tls_serialize_detached().unwrap();

        //tracing::info!("Sending Welcome to {}", user_name);
        let topic = format!("cgka/group/{group_name}");
        let client = self.async_client.clone();
        thread::spawn(move || {
            block_on(async {
                client.publish(topic, QoS::ExactlyOnce, false, serialized_msg)
                    .await
            }).map_err(|e| format!{"Error publishing ACK: {:?}", e}).expect("Error sending ACK");
        }).join().expect("Thread panicked");


        Ok(())
    }

    fn send_welcome(&self, welcome_msg: &GroupMessage, user_name: String) -> Result<(), String> {
        let ds_message = DSMessage::GroupMessage(welcome_msg.clone());
        let serialized_msg = ds_message.tls_serialize_detached().unwrap();

        let topic = format!("cgka/welcome/{user_name}");

        let client = self.async_client.clone();
        thread::spawn(move || {
            block_on(async {
                client.publish(topic, QoS::ExactlyOnce, false,  serialized_msg)
                    .await
            }).map_err(|e| format!{"Error publishing Welcome: {:?}", e}).expect("Error sending welcome");
        }).join().expect("Thread panicked");

        Ok(())
    }

    /// Send a group message.
    #[tokio::main]
    async fn send_msg(&self, group_msg: &GroupMessage, group_name: String) -> Result<(), String> {
        let ds_message = DSMessage::GroupMessage(group_msg.clone());
        let serialized_msg = ds_message.tls_serialize_detached().unwrap();

        let topic = format!("cgka/group/{group_name}");

        self.async_client.publish(topic, QoS::ExactlyOnce, false,  serialized_msg)
            .await.map_err(|e| format!{"Error publishing message: {:?}", e})?;

        tracing::info!("Published message to group {}", group_name);
        Ok(())
    }
}

impl MqttBroker {
    pub fn new_from_client(username: String, async_client: AsyncClient, tx: Sender<MQTTQueueMessage>) -> Self{
        Self {
            username,
            async_client,
            tx
        }
    }
}