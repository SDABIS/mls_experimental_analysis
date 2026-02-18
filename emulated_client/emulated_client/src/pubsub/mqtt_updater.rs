use std::sync::{Arc, Mutex};
use rumqttc::{Event, EventLoop, AsyncClient, QoS, Outgoing};
use crate::pubsub::parse_message;
use crate::user::User;
use tokio::select;
use tokio::sync::{mpsc::Receiver};
use std::thread;
use std::collections::{HashMap};
use openmls_traits::OpenMlsProvider;
use ds_lib::{DSMessage, WelcomeAcknowledgement};
use tls_codec::Serialize;

pub struct MqttUpdater<P: OpenMlsProvider> {
    event_loop: EventLoop,
    async_client: AsyncClient,
    users: HashMap<String, Arc<Mutex<User<P>>>>,
    subscriptions: HashMap<String, Vec<String>>,
    command_receiver: Receiver<MQTTQueueMessage>,
    pending_subscriptions: Vec<(String, String, Option<usize>)>,
    current_handles: HashMap<String, thread::JoinHandle<()>>,
    //barrier: Option<Arc<Barrier>>,
}

#[derive(Debug)]
pub enum MQTTQueueMessage {
    Subscribe(String, String),
    Unsubscribe(String, String),
}

impl<P: OpenMlsProvider + std::marker::Send + 'static> MqttUpdater<P> {
    pub fn new(users: HashMap<String, Arc<Mutex<User<P>>>>, rx: Receiver<MQTTQueueMessage>, event_loop: EventLoop, async_client: AsyncClient) -> Self {

        let mut subscriptions = HashMap::new();
        for user_name in users.keys() {
            subscriptions.insert(user_name.clone(), Vec::new());
        }
        let current_handles = HashMap::new();

        Self {
            users,
            event_loop,
            async_client,
            subscriptions,
            command_receiver: rx,
            current_handles,
            pending_subscriptions: Vec::new(),
            //barrier: None,
        }
    }

    #[tokio::main]
    pub async fn run(&mut self) {
        loop {
            select!{
                event = self.event_loop.poll() => {
                    if let Ok(notification) = event
                    {
                        match notification {
                            Event::Incoming(packet) => {
                                match packet {
                                    rumqttc::Packet::Publish(publish) => {
                                        self.deliver_to_users(publish.topic.clone(), publish.payload.to_vec()).await;
                                    }
                                    rumqttc::Packet::SubAck(packet) => {

                                        // Find the pending subscription with the matching packet ID
                                        let pending = self.pending_subscriptions.iter()
                                            .find(|(_, _, id)| *id == Some(packet.pkid as usize))
                                            .map(|(u, t, _)| (u.clone(), t.clone()));

                                        if let Some((user, topic)) = pending {

                                            self.pending_subscriptions.retain(|(_, _, id)| id != &Some(packet.pkid as usize));

                                            if topic.starts_with("cgka/group/") {
                                                tracing::debug!("Subscription acknowledged by {} for topic: {}", user, topic);

                                                let group_name = topic.trim_start_matches("cgka/group/").to_string();
                                                let ack = WelcomeAcknowledgement {
                                                    sender: user.clone(),
                                                    group: group_name.clone(),
                                                };

                                                let ds_message = DSMessage::WelcomeAcknowledgement(ack.clone());
                                                let serialized_msg = ds_message.tls_serialize_detached().unwrap();

                                                //tracing::info!("Sending Welcome to {}", user_name);
                                                let topic = format!("cgka/group/{group_name}");
                                                self.async_client.publish(topic, QoS::ExactlyOnce, false, serialized_msg).await.expect("Error sending ACK");
                                            }

                                        } else {
                                            tracing::warn!("Received SubAck for unknown subscription ID: {}", packet.pkid);
                                            if self.pending_subscriptions.len() == 1 {
                                                tracing::warn!("Pending subscriptions: {:?}", self.pending_subscriptions);
                                            }
                                        }

                                    }
                                    _ => {}

                                }
                            }
                            Event::Outgoing(packet) => {
                                match packet {
                                    Outgoing::Subscribe(a) => {

                                        if let Some(slot) = self.pending_subscriptions.iter_mut().find(|(_, _, id)| id.is_none()) {
                                            slot.2 = Some(a as usize);
                                        } else {
                                            tracing::warn!(
                                                "Outgoing subscription {:?} not recognised. Pending subscriptions: {:?}",
                                                a,
                                                self.pending_subscriptions
                                            );
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    else {
                        let error = event.unwrap_err();
                        tracing::error!("Connection error: {:?}", error);
                        panic!("Connection error: {:?}", error);
                    }
                },
                Some(msg) = self.command_receiver.recv() => {
                    match msg {
                        MQTTQueueMessage::Subscribe(user, topic) => {
                            self.subscriptions.get_mut(&user).unwrap().push(topic.clone());
                            self.async_client.subscribe(topic.clone(), QoS::ExactlyOnce).await.expect("Error subscribing to topic");
                            tracing::debug!("User {} subscribed to topic {}", user, topic);

                            self.pending_subscriptions.push((user, topic.clone(), None));
                        }
                        MQTTQueueMessage::Unsubscribe(user, topic) => {
                            if let Some(topics) = self.subscriptions.get_mut(&user) {
                                topics.retain(|t| t != &topic);
                            }

                            // if no user is subscribed to the topic, unsubscribe from broker
                            let mut subscribed = false;
                            for (_user, topics) in &self.subscriptions {
                                if topics.contains(&topic) {
                                    subscribed = true;
                                    break;
                                }
                            }
                            if !subscribed {
                                tracing::info!("No users subscribed to topic {}, unsubscribing from broker", topic);
                                self.async_client.unsubscribe(topic).await.expect("Error unsubscribing from topic");
                            }
                        }
                    }
                }
            }
        }
    }

    async fn deliver_to_users(&mut self, topic: String, message: Vec<u8>) {

        for (username, subscriptions) in &self.subscriptions {
            if subscriptions.contains(&topic) {

                let user = self.users.get(username).unwrap();

                let user_thread = Arc::clone(user);
                let topic_thread = topic.to_string().clone();
                let username_thread = username.clone();
                let mut message_thread = message.clone();

                let previous_handle = self.current_handles.remove(username);

                let handle = thread::spawn(move || {

                    if let Some(h) = previous_handle
                    {
                        h.join().expect("Thread panicked");
                    }

                    {
                        let mut user = user_thread.lock().unwrap();

                        tracing::span!(tracing::Level::INFO, "listener", user = username_thread).in_scope(|| {
                            parse_message(&mut message_thread, topic_thread, username_thread, &mut user).expect("Error processing message");
                        });
                        drop(user)
                    }

                });

                self.current_handles.insert(username.clone(), handle);

            }
        }

    }
}