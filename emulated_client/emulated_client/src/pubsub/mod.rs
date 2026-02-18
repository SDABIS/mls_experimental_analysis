use std::str::from_utf8;
use openmls_traits::OpenMlsProvider;
use tls_codec::{Deserialize};
use ds_lib::{DSMessage, GroupMessage, WelcomeAcknowledgement};
use crate::user::{User};

pub mod mqtt_updater;
pub mod gossipsub_updater;
pub mod mqtt_broker;
pub mod gossipsub_broker;

pub fn parse_message<P: OpenMlsProvider>(message: &mut Vec<u8>, topic: String, user_name: String, user: &mut User<P>) -> Result<(), String> {
    let message = DSMessage::tls_deserialize(&mut message.as_slice()).unwrap();
    match message {
        DSMessage::WelcomeAcknowledgement(ack) => {
            user.process_ack(ack)
        }
        DSMessage::GroupMessage(message) => {
            let recipients = message.recipients.clone().iter()
                .map(|r| from_utf8(r.as_slice()).unwrap().to_string()).collect::<Vec<String>>();

            if !recipients.contains(&user_name) && topic.starts_with("cgka/welcome") {
                return Ok(());
            }

            match user.process_in_message(message.clone()) {
                Ok(action_record) => {
                    if let Some(record) = action_record {
                        user.write_timestamp(record);
                    }
                    Ok(())
                }
                Err(e) => {
                    let sender = from_utf8(message.sender()).unwrap().to_string();
                    tracing::error!("Error processing message from: {}: {}",sender, e);
                    Err(e)
                },
            }
        }
    }
}

pub trait Broker {
    fn subscribe(&mut self, group_name: String) -> Result<(), String>;
    fn unsubscribe(&mut self, group_name: String) -> Result<(), String>;
    fn subscribe_welcome(&mut self, user_name: String) -> Result<(), String>;
    fn send_ack(&self, ack: &WelcomeAcknowledgement) -> Result<(), String>;
    fn send_welcome(&self, welcome_msg: &GroupMessage, user_name: String) -> Result<(), String>;
    fn send_msg(&self, group_msg: &GroupMessage, group_name: String) -> Result<(), String>;
}
