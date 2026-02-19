
use std::borrow::Borrow;
use std::collections::HashSet;
use std::vec;
use std::{cell::RefCell, collections::HashMap, str, thread};
use std::cmp::PartialEq;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use chrono::prelude::*;
use cpu_time::ThreadTime;
use futures::executor::block_on;
use ds_lib::{ClientKeyPackages, GroupInfoAndTree, GroupMessage, WelcomeAcknowledgement};
use openmls::prelude::*;
use rand::Rng;
use std::time::Duration;

use std::fs::OpenOptions;
use std::io::Write;
use openmls_traits::{OpenMlsProvider};
use tls_codec::{TlsByteVecU8};
use openmls::framing::errors::MessageDecryptionError;
use openmls::framing::errors::MessageDecryptionError::{AeadError};
use openmls::prelude::group_info::{GroupInfo};
use crate::pubsub::{Broker, mqtt_broker::MqttBroker, gossipsub_broker::GossipSubBroker};
use crate::client_agent::{ActionRecord, CGKAAction};
use openmls::prelude::Propose;
use rand::seq::SliceRandom;
use url::Url;
use crate::orchestrator::{NextAction, Orchestrator};
use crate::config::{AuthorizationPolicy, Directory, OrchestratedParameters};
use tokio::sync::mpsc::Sender;
use std::hint::black_box;

use super::{
    network::backend::Backend, conversation::Conversation, conversation::ConversationMessage,
    identity::Identity
};

pub const CIPHERSUITE: Ciphersuite = Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;
const FILE_BUFFER_SIZE: usize = 50;

pub enum AgentType {
    Orchestrator(OrchestratedParameters, Sender<String>, HashMap<String, Orchestrator>),
    Independent,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Contact {
    username: String,
    id: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StoredWelcome {
    welcome: MlsMessageOut,
    recipients: Vec<String>
}

#[derive(Debug)]
pub struct Group {
    group_name: String,
    conversation: Conversation,
    mls_group: RefCell<MlsGroup>,
}

#[derive(Debug, Clone)]
pub struct EpochChange {
    pub(crate) timestamp: i64,
    pub(crate) epoch: u64,
}

pub struct User<P: OpenMlsProvider> {
    pub(crate) username: String,
    pub(crate) contacts: HashMap<Vec<u8>, Contact>,
    pub(crate) groups: RefCell<HashMap<String, Group>>,
    group_list: HashSet<String>,
    pub(crate) identity: RefCell<Identity>,
    pub(crate) pending_commits: HashMap<String, (Option<GroupInfo>, Option<StoredWelcome>, ActionRecord)>,
    backend: Backend,
    ds: DeliveryService,
    crypto: P,
    active: HashMap<String, (bool, u64)>,

    agent_type: AgentType,
    
    file_buffer: Vec<ActionRecord>
}

#[derive(PartialEq)]
pub enum PostUpdateActions {
    None,
    Remove,
}

#[derive(Default, Debug)]
pub enum DeliveryService {
    #[default]
    Request,
    PubSubMQTT(Arc<Mutex<MqttBroker>>),
    GossipSub(Arc<Mutex<GossipSubBroker>>, Directory),
}

impl Clone for DeliveryService {
    fn clone(&self) -> Self {
        match self {
            DeliveryService::Request => DeliveryService::Request,
            DeliveryService::PubSubMQTT(broker) => DeliveryService::PubSubMQTT(Arc::clone(broker)),
            DeliveryService::GossipSub(broker, dir) => DeliveryService::GossipSub(Arc::clone(broker), dir.clone()),
        }
    }
}

impl<P: OpenMlsProvider> User<P> {

/// Create a new user with the given name and a fresh set of credentials.
    pub fn new_orchestrated(crypto: P, username: String, backend_url: Url, ds: DeliveryService, sender: Sender<String>, orchestrator_params: OrchestratedParameters) -> Self {
        let agent_type = AgentType::Orchestrator(orchestrator_params.clone(), sender, HashMap::new());
        //let crypto = OpenMlsRustPersistentCrypto::default();
        Self {
            username: username.clone(),
            groups: RefCell::new(HashMap::new()),
            group_list: HashSet::new(),
            contacts: HashMap::new(),
            identity: RefCell::new(Identity::new(CIPHERSUITE, &crypto, username.as_bytes())),
            pending_commits: HashMap::new(),
            backend: Backend::new(backend_url),
            ds,
            crypto,
            agent_type,
            active: HashMap::new(),
            file_buffer: Vec::new(),
        }
    }

    pub fn new_independent(crypto: P, username: String, backend_url: Url, ds: DeliveryService) -> Self {
        let agent_type = AgentType::Independent;
        //let crypto = OpenMlsRustPersistentCrypto::default();
        Self {
            username: username.clone(),
            groups: RefCell::new(HashMap::new()),
            group_list: HashSet::new(),
            contacts: HashMap::new(),
            identity: RefCell::new(Identity::new(CIPHERSUITE, &crypto, username.as_bytes())),
            pending_commits: HashMap::new(),
            backend: Backend::new(backend_url),
            ds,
            crypto,
            agent_type,
            active: HashMap::new(),
            file_buffer: Vec::new(),
        }
    }

    /// Add a key package to the user identity and return the pair [key package
    /// hash ref , key package]
    pub fn add_key_package(&self) -> (Vec<u8>, KeyPackage) {
        let kp = self
            .identity
            .borrow_mut()
            .add_key_package(CIPHERSUITE, &self.crypto);
        (
            kp.hash_ref(self.crypto.crypto())
                .unwrap()
                .as_slice()
                .to_vec(),
            kp,
        )
    }

    /// Get a member
    fn find_member_index(&self, name: String, group: &Group) -> Result<LeafNodeIndex, String> {
        let mls_group = group.mls_group.borrow();
        for Member {
            index,
            encryption_key: _,
            signature_key: _,
            credential,
        } in mls_group.members()
        {
            if credential.identity() == name.as_bytes() {
                return Ok(index);
            }
        }
        Err("Unknown member".to_string())
    }

    /// Get the key packages fo this user.
    pub fn key_packages(&self) -> Vec<(Vec<u8>, KeyPackage)> {
        let kpgs = self.identity.borrow().kp.clone();
        Vec::from_iter(kpgs)
    }

    pub fn group_exists(&self, group_name: String) -> Result<bool, String> {
        match self.ds {
            DeliveryService::Request | DeliveryService::PubSubMQTT(_) | DeliveryService::GossipSub(_, Directory::Server) => {
                self.backend.group_exists(group_name)
            }
            DeliveryService::GossipSub(ref broker, Directory::Kademlia) => {
                let broker = broker.lock().unwrap();
                let result = broker.group_exists(group_name.clone())?;
                drop(broker);
                Ok(result)
            }
        }
    }

    pub fn get_group_info(&self, group_name: String) -> Result<GroupInfoAndTree, String> {
        match self.ds {
            DeliveryService::Request | DeliveryService::PubSubMQTT(_) | DeliveryService::GossipSub(_, Directory::Server) => {
                self.backend.group_info(self.username.clone(), group_name)
            }
            DeliveryService::GossipSub(ref broker, Directory::Kademlia) => {
                let broker = broker.lock().unwrap();
                let result = broker.group_info(group_name.clone())?;
                drop(broker);
                Ok(result)
            }
        }
    }

    pub fn consume_key_package(&self, contact: &Contact) -> Result<KeyPackageIn, String> {
        match self.ds {
            DeliveryService::Request | DeliveryService::PubSubMQTT(_) | DeliveryService::GossipSub(_, Directory::Server) => {
                self.backend.consume_key_package(&contact.id)
            }
            DeliveryService::GossipSub(ref broker, Directory::Kademlia) => {

                let broker = broker.lock().unwrap();
                let result = broker.consume_key_package(contact.username.clone())?;
                drop(broker);
                Ok(result)
            }
        }

    }

    pub fn register(&self) -> Result<(), String> {
         match self.backend.register_client(self.username.clone(), &self.identity.borrow()) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error creating user: {:?}", e))
        }
    }

    pub fn subscribe_welcome(&mut self) -> Result<(), String> {
        match self.ds {
            DeliveryService::Request => {Ok(())}
            DeliveryService::PubSubMQTT(ref broker) => {
                let mut broker = broker.lock().unwrap();
                broker.subscribe_welcome(self.username.clone())?;
                drop(broker);
                Ok(())
            }

            DeliveryService::GossipSub(ref broker, _) => {
                let mut broker = broker.lock().unwrap();
                broker.subscribe_welcome(self.username.clone())?;
                drop(broker);
                Ok(())
            }
        }
    }

    fn subscribe(&mut self, group_name: String) -> Result <(), String> {
        tracing::info!("Subscribing to {}", group_name);

        match self.ds {
            DeliveryService::Request => {Ok(())}
            DeliveryService::PubSubMQTT(ref broker) => {
                let mut broker = broker.lock().unwrap();
                broker.subscribe(group_name.clone())?;
                drop(broker);
                Ok(())
            }

            DeliveryService::GossipSub(ref broker, _) => {
                let mut broker = broker.lock().unwrap();
                broker.subscribe(group_name.clone())?;
                drop(broker);
                Ok(())
            }
        }
    }

    pub fn unsubscribe(&mut self, group_name: String) -> Result <(), String> {
        tracing::debug!("Unsubscribing from {}", group_name);


        match self.ds {
            DeliveryService::Request => {Ok(())}
            DeliveryService::PubSubMQTT(ref broker) => {
                let mut broker = broker.lock().unwrap();
                broker.unsubscribe(group_name.clone())?;
                drop(broker);
                Ok(())
            }
            DeliveryService::GossipSub(ref broker, _)=> {
                let mut broker = broker.lock().unwrap();
                broker.unsubscribe(group_name.clone())?;
                drop(broker);
                Ok(())
            }
        }
    }

    fn mls_recipients (&self, mls_group: &MlsGroup) -> Vec<Vec<u8>> {
        let mut recipients = Vec::new();

        for Member {
            index: _,
            encryption_key: _,
            signature_key: _,
            credential,
        } in mls_group.members()
        {
            if self
                .identity
                .borrow()
                .credential_with_key
                .credential
                .identity()
                != credential.identity()
            {
                tracing::debug!(
                    "Searching for contact {:?}",
                    from_utf8(credential.identity()).unwrap()
                );
                let contact = match self.contacts.get(&credential.identity().to_vec()) {
                    Some(c) => c.id.clone(),
                    None => {

                        let id = credential.identity().to_vec();
                        id
                    }
                };
                recipients.push(contact);
            }
        }
        recipients
    }

    /// Get a list of clients in the group to send messages to.
    fn recipients(&self, group: &Group) -> Vec<Vec<u8>> {

        let mls_group = group.mls_group.borrow();
        self.mls_recipients(mls_group.borrow())
    }

    pub fn members_of_group(&self, group_name: String) -> Vec<String> {
        let groups = self.groups.borrow();
        let group = match groups.get(&group_name) {
            Some(g) => g,
            None => return Vec::new(),
        };

        let recipients = self.recipients(group);

        let mut members = Vec::new();
        for recipient in recipients {
            let contact = self.contacts.get(&recipient).cloned().unwrap_or(
                Contact {
                    username: from_utf8(&recipient).unwrap().to_string(),
                    id: recipient,
                }
            );
            members.push(contact.username);
        }

        members
    }

    pub fn number_of_members(&self, group_name: String) -> usize {
        let groups = self.groups.borrow();
        let group = match groups.get(&group_name) {
            Some(g) => g,
            None => return 1,
        };

        let count = group.mls_group.borrow().members().count();

        count
    }

    pub fn not_members_of_group(&self, group_name: String) -> Vec<String> {
        let groups = self.groups.borrow();
        let group = match groups.get(&group_name) {
            Some(g) => g,
            None => return Vec::new(),
        };

        let recipients = self.recipients(group);

        let mut members = Vec::new();
        for contact in self.contacts.values() {
            if !recipients.contains(&contact.id) {
                members.push(contact.username.clone());
            }
        }

        members
    }

    pub fn list_of_groups(&self) -> Vec<String> {
        self.groups.borrow().keys().cloned().collect()
    }

    pub fn crypto(&self) -> &impl OpenMlsProvider {
        &self.crypto
    }

    /// Return the last 100 messages sent to the group.
    pub fn read_msgs(
        &self,
        group_name: String,
    ) -> Result<Option<Vec<ConversationMessage>>, String> {
        let groups = self.groups.borrow();
        groups.get(&group_name).map_or_else(
            || Err("Unknown group".to_string()),
            |g| {
                Ok(g.conversation
                    .get(100)
                    .map(|messages: &[ConversationMessage]| messages.to_vec()))
            },
        )
    }

    /// Create a new key package and publish it to the delivery server
    pub fn create_kp(&self) -> Result<(), String> {

        let kp = self.add_key_package();
        
        match self.ds {
            DeliveryService::Request | DeliveryService::PubSubMQTT(_) | DeliveryService::GossipSub(_, Directory::Server) => {
                let ckp = ClientKeyPackages(
                    vec![kp]
                        .into_iter()
                        .map(|(b, kp)| (b.into(), KeyPackageIn::from(kp)))
                        .collect::<Vec<(TlsByteVecU8, KeyPackageIn)>>()
                        .into(),
                );

                match self.backend.publish_key_packages(self.identity.borrow().identity(), &ckp) {
                    Ok(()) => Ok(()),
                    Err(e) => Err(format!("Error sending new key package: {e:?}"))
                }
            }
            DeliveryService::GossipSub(ref broker, Directory::Kademlia) => {
                let broker = broker.lock().unwrap();
                broker.publish_key_package(self.username.clone(), kp.1)?;
                drop(broker);
                Ok(())
            }
        }

    }

    /// Send an application message to the group.
    pub fn send_application_msg(&mut self, msg: String, group_name: String) -> Result<(), String> {
        if self.pending_commits.contains_key(&group_name) {
            return Err("There is a pending commit".to_string());
        }

        let groups = self.groups.borrow();
        let group = match groups.get(&group_name) {
            Some(g) => g,
            None => return Err("Unknown group".to_string()),
        };

        let message_out = group
            .mls_group
            .borrow_mut()
            .create_message(&self.crypto, &self.identity.borrow().signer, msg.as_bytes())
            .map_err(|e| format!("{e}"))?;

        let recipients = match self.ds {
            DeliveryService::Request => self.recipients(group),
            _ => Vec::new()
        };

        self.send_to_group(group_name.clone(), message_out, &recipients)?;
        
        tracing::debug!(" >>> send: {:?}", msg);

        Ok(())
    }

    pub fn add_contact(&mut self, contact: Contact) -> Result<(), String> {
        let client_id = contact.id.clone();
        tracing::debug!(
                        "update::Processing client for contact {:?}",
                        from_utf8(&client_id).unwrap()
                    );
        if contact.id != self.identity.borrow().identity()
            && self
            .contacts
            .insert(
                contact.id.clone(),
                Contact {
                    username: contact.username,
                    id: contact.id,
                },
            )
            .is_some()
        {
            tracing::debug!(
                            "update::added client to contact {:?}",
                            from_utf8(&client_id).unwrap()
                        );
            tracing::trace!("Updated client {}", "");
        }

        Ok(())
    }

    /// Update the user clients list.
    /// It updates the contacts with all the clients known by the server
    pub fn update_clients(&mut self) -> Result<(), String> {
        match self.backend.list_clients() {
            Ok(mut v) => {
                for c in v.drain(..) {
                    self.add_contact(Contact {
                        username: c.client_name,
                        id: c.id,
                    })?;
                }
            }
            Err(e) => tracing::debug!("update_clients::Error reading clients from DS: {:?}", e),
        }
        tracing::debug!("update::Processing clients done, contact list is:");
        for contact_id in self.contacts.borrow().keys() {
            tracing::debug!(
                "update::Parsing contact {:?}",
                from_utf8(contact_id).unwrap()
            );
        }

        Ok(())
    }

    fn process_protocol_message(&mut self, message: ProtocolMessage, sender: String)
                                    -> Result<(Option<ActionRecord>, PostUpdateActions, Option<GroupId>), String>
    {
        let group_name = from_utf8(message.group_id().as_slice()).unwrap();
        let mut groups = self.groups.borrow_mut();

        let group = match groups.get_mut(group_name) {
            Some(g) => g,
            None => {
                tracing::error!(
                    "Error getting group {:?} for a message. Dropping message.",
                    message.group_id()
                );
                return Ok((None, PostUpdateActions::None, None));
            }
        };

        let timestamp = Utc::now().timestamp_nanos_opt().unwrap();

        let group_epoch = group.mls_group.borrow_mut().epoch().as_u64();
        let message_epoch = message.epoch().as_u64();

        let mut epoch_change = EpochChange {timestamp, epoch: message_epoch};

        if group_epoch < message_epoch {

            // User has become desync-ed
            let message_type = message.content_type();

            if ContentType::Application == message_type
                || ContentType::Proposal == message_type {
                return Ok((None, PostUpdateActions::None, None));
            }

            let previous_state = self.active.insert(group_name.to_string(), (false, message_epoch+1));
            epoch_change.epoch +=1;

            if let Some((active, epoch)) = previous_state {
                if active {
                    tracing::info!("Has become DESYNC-ED in {} by message sent by {}.", group_name, sender);
                }
                if epoch == message_epoch || active {
                    // Keep logging received messages for the current epoch
                    let action_record = ActionRecord {
                        group_name: group_name.to_string(),
                        action: CGKAAction::Process(sender),
                        epoch_change,
                        elapsed_time: 0,
                        num_users: 0,
                    };
                    return Ok((Some(action_record), PostUpdateActions::None, None));
                }
                else {
                    // Ignore messages from previous epochs
                    return Ok((None, PostUpdateActions::None, None));

                }
            }

        }

        let now = ThreadTime::now();

        let (processed_message, time) = {
            let mut mls_group = group.mls_group.borrow_mut();
            let (processed_message, time) = match mls_group.process_message(&self.crypto, message.clone()) {
                Ok((msg, time)) => {(msg, time)},
                Err(e) => {
                    // Conflict
                    if ProcessMessageError::ValidationError(ValidationError::WrongEpoch) == e
                        || ProcessMessageError::ValidationError(ValidationError::UnableToDecrypt(AeadError)) == e
                        || ProcessMessageError::ValidationError(ValidationError::InvalidSignature) == e
                        || ProcessMessageError::ValidationError(ValidationError::UnableToDecrypt(MessageDecryptionError::GenerationOutOfBound)) == e
                    {

                        if ContentType::Application == message.content_type() {
                            return Ok((None, PostUpdateActions::None, None));
                        }

                        tracing::info!("CONFLICT IN {} by message sent by {}.", group.group_name, sender);

                        return Ok((None, PostUpdateActions::None, None));
                    }
                    else {
                        tracing::error!(
                    "Error processing unverified message: {:?} -  Dropping message.",
                    e);
                        return Err(e.to_string());

                    }
                }
            };

            (processed_message, time)
        };

        let processed_message_credential: Credential = processed_message.credential().clone();

        let sender_name = from_utf8(processed_message_credential.identity()).unwrap();

        let (elapsed, action) = match processed_message.into_content() {
            ProcessedMessageContent::ApplicationMessage(application_message) => {

                let conversation_message = ConversationMessage::new(
                    String::from_utf8(application_message.into_bytes()).unwrap().clone(),
                    sender_name.to_string(),
                );
                tracing::info!("RECEIVED APPLICATION MESSAGE by {}", conversation_message.author);

                group.conversation.add(conversation_message);

                (None, None)
            }
            ProcessedMessageContent::ProposalMessage(proposal_ptr) => {
                tracing::info!("PROCESSED PROPOSAL from {} IN {}.", sender_name, group.group_name);

                group.mls_group.borrow_mut().store_pending_proposal(
                    self.crypto().storage(),
                    *proposal_ptr)
                    .map_err(|e| format!("Error storing proposal: {e}"))?;
                (None, Some(CGKAAction::StoreProp(sender.clone())))
            }
            ProcessedMessageContent::ExternalJoinProposalMessage(_external_proposal_ptr) => {
                // intentionally left blank.
                (None, None)
            }
            ProcessedMessageContent::StagedCommitMessage(commit_ptr) => {
                let mut mls_group = group.mls_group.borrow_mut();
                let commit: StagedCommit = *commit_ptr;

                let new_members = commit.add_proposals().map(|add_proposal| {
                    from_utf8(add_proposal.add_proposal().key_package().leaf_node().credential().clone().identity())
                        .unwrap()
                        .to_string()
                }).collect::<Vec<String>>();
                let removed_members = commit.remove_proposals().map(|remove_proposal| {
                    let index = remove_proposal.remove_proposal().removed();
                    let removed_member = from_utf8(
                        mls_group.member_at(index).clone().unwrap().credential.identity()
                    ).unwrap().to_string();
                    removed_member
                }).collect::<Vec<String>>();
                let num_group_members = mls_group.members().count();
                let num_of_new_users = new_members.len();
                let num_of_removed_users = removed_members.len();

                let transcript_hash = commit
                    .group_context()
                    .transcript_hash();

                let mut remove_proposal: bool = false;
                if commit.self_removed() {
                    remove_proposal = true;
                }

                let merge_time = mls_group.merge_staged_commit(&self.crypto, commit)
                    .map_err(|e| format!("Error merging commit: {:?}", e))?;

                // Stop counting time
                let elapsed = now.elapsed().as_micros();
                

                let next_action = match self.agent_type {
                    AgentType::Independent => {
                        //NextAction::Listen
                        // TEST: 99% chance of going inactive when removed
                        let mut rng = rand::thread_rng();
                        if rng.gen::<f64>() < 0.5 {
                            NextAction::Inactive
                        }
                        else {
                            NextAction::Listen
                        }
                    }
                    AgentType::Orchestrator(_, _, ref mut orchestrators) => {
                        //let mls_group = group.mls_group.borrow();

                        let orchestrator = orchestrators.get_mut(group_name).unwrap();

                        orchestrator.remove_members(removed_members);
                        let next_action = orchestrator.process_commit(
                            &transcript_hash,
                            num_group_members + num_of_new_users - num_of_removed_users,
                            new_members
                        );

                        next_action
                    }
                };

                epoch_change.epoch += 1;
                if remove_proposal || next_action == NextAction::Inactive {
                    let elapsed = now.elapsed().as_micros();

                    tracing::info!("Going inactive in {}", group_name);

                    tracing::debug!("update::Processing StagedCommitMessage removed from group {} ", group.group_name);


                    return Ok((
                        Some(ActionRecord {
                            group_name:group_name.to_string(),
                            epoch_change: EpochChange {
                                epoch: group_epoch + 1, timestamp
                            },
                            action: CGKAAction::Process(sender.clone()),
                            elapsed_time: elapsed,
                            num_users: mls_group.members().count(),
                        } ),
                        PostUpdateActions::Remove,
                        Some(mls_group.group_id().clone()),
                    ));
                }
                
                //let result = CGKAAction::Process(sender.clone());
                let result = CGKAAction::DeepProcess(sender.clone(), time.unwrap_or_default(), merge_time);

                let members = mls_group.members().count();
                tracing::info!("PROCESSED COMMIT from {} IN {}. Epoch: {}. Members: {})", sender_name, group.group_name,
                    mls_group.epoch().as_u64(), members);


                match next_action {
                    NextAction::Commit => {
                        self.send_to_agent(group_name.to_string())?;
                    }
                    _ => {}
                }

                (Some(elapsed), Some(result))
            }
        };

        let elapsed = elapsed.unwrap_or_else(|| now.elapsed().as_micros());
        
        let action_record = match action {
            Some(cgka_action) => Some(ActionRecord {
                group_name: group_name.to_string(),
                action: cgka_action,
                epoch_change,
                elapsed_time: elapsed,
                num_users: group.mls_group.borrow().members().count(),
            }),
            None => None
        };
        Ok((action_record, PostUpdateActions::None, None))
    }

    pub fn process_in_message(&mut self, group_message: GroupMessage)
        -> Result<Option<ActionRecord>, String>{

        let GroupMessage {
            sender,
            msg: message,
            ratchet_tree,
            recipients,
            active_members
        } = group_message;

        tracing::debug!("Reading message format {:#?} ...", message.wire_format());

        let sender = from_utf8(sender.as_slice()).unwrap().to_string();

        if sender == self.username {
            let group_name = from_utf8(message.into_protocol_message().unwrap().group_id().as_slice()).unwrap().to_string();
            tracing::debug!("Received message sent by self");
            self.confirm_commit(group_name.clone())?;
            return Ok(None);
        }

        let welcome_size = message.tls_serialized_len() + ratchet_tree.as_ref().map_or(0, |rt| rt.tls_serialized_len());
        match message.extract() {
            MlsMessageBodyIn::Welcome(welcome) => {

                let ratchet_tree = ratchet_tree.ok_or("Welcome message must have ratchet tree")?;

                let (group_name, epoch_change, elapsed_time) = self.join_group(welcome, ratchet_tree)?;
                self.active.insert(group_name.clone(), (true, 0));
                let num_users = self.groups.borrow().get(&group_name).unwrap().mls_group.borrow().members().count();

                match &mut self.agent_type {
                    AgentType::Independent => {
                        self.subscribe(group_name.clone())?;
                    }
                    AgentType::Orchestrator(params, _, ref mut orchestrators) => {
                        let mut orchestrator = if let Some(active_members) = active_members {
                            let active_members = active_members.into_iter().map(
                                |am| from_utf8(am.as_slice()).unwrap().to_string()
                            ).collect::<Vec<String>>();
                            let orchestrator = Orchestrator::new(
                                self.username.clone(),
                                params.clone(),
                                active_members,
                            );

                            orchestrator
                        }
                        else {
                            unreachable!("Welcome messages must have active members");
                        };

                        let (num_group_members, transcript_hash, new_members) ={
                            let groups = self.groups.borrow();
                            let group_entry = groups.get(&group_name).unwrap();
                            let group = group_entry.mls_group.borrow();

                            let num_group_members = group.members().count().clone();
                            let transcript_hash = group.transcript_hash();

                            let new_members = recipients.iter().map(
                                |r| {
                                    let parsed_recipient = from_utf8(r.as_slice()).unwrap().to_string();
                                    parsed_recipient
                                }
                            ).collect::<Vec<String>>();

                            (num_group_members, transcript_hash, new_members)
                        };

                        let next_action = orchestrator.process_commit(
                            &transcript_hash,
                            num_group_members,
                            new_members
                        );
                        orchestrators.insert(group_name.clone(), orchestrator);

                        match next_action {
                            NextAction::Inactive => {
                                tracing::info!("Going inactive in {}", group_name);
                                self.send_ack(group_name.clone());
                                self.groups.borrow_mut().remove(&group_name);
                            }
                            NextAction::Listen => {
                                self.subscribe(group_name.clone())?;
                            }
                            NextAction::Commit => {
                                self.subscribe(group_name.clone())?;
                                self.send_to_agent(group_name.clone())?;
                            }
                        }
                    }
                }

                let action_record = ActionRecord {
                    group_name: group_name.clone(),
                    epoch_change,
                    action: CGKAAction::Welcome(sender.clone(), welcome_size),
                    elapsed_time,
                    num_users,
                };
                Ok(Some(action_record))
            }
            MlsMessageBodyIn::PrivateMessage(private_message) => {
                match self.process_protocol_message(private_message.into(), sender.clone()) {
                    Ok(p) => {
                        if p.1 == PostUpdateActions::Remove {

                            self.flush_logs();

                            return match p.2 {
                                Some(gid) => {
                                    self.unsubscribe(from_utf8(gid.as_slice()).unwrap().to_string())
                                        .map_err(|e| format!("Error unsubscribing: {e}"))?;
                                    let mut grps = self.groups.borrow_mut();
                                    grps.remove_entry(from_utf8(gid.as_slice()).unwrap());
                                    self.group_list
                                        .remove(from_utf8(gid.as_slice()).unwrap());

                                    Ok(p.0)
                                }
                                None => {
                                    Err("update::Error post update remove must have a group id".to_string())
                                }
                            }
                        }

                        Ok(p.0)
                    }
                    Err(e) => {
                        Err(e)
                    }
                }
            },
            MlsMessageBodyIn::PublicMessage(public_message) => {
                match self.process_protocol_message(public_message.into(), sender.clone()) {
                    Ok(p) => {
                        if p.1 == PostUpdateActions::Remove {

                            self.flush_logs();

                            return match p.2 {
                                Some(gid) => {
                                    self.unsubscribe(from_utf8(gid.as_slice()).unwrap().to_string())
                                        .map_err(|e| format!("Error unsubscribing: {e}"))?;
                                    let mut grps = self.groups.borrow_mut();
                                    grps.remove_entry(from_utf8(gid.as_slice()).unwrap());
                                    self.group_list
                                        .remove(from_utf8(gid.as_slice()).unwrap());

                                    Ok(p.0)
                                }
                                None => {
                                    Err("update::Error post update remove must have a group id".to_string())
                                }
                            }
                        }

                        Ok(p.0)
                    }
                    Err(e) => {
                        Err(e)
                    }
                }
            }
            _ => panic!("Unsupported message type"),
        }
    }

    pub fn fetch_updates(
        &mut self
    ) -> Result<Vec<ConversationMessage>, String> {
        tracing::debug!("Updating {} ...", self.username);

        let messages_out: Vec<ConversationMessage> = Vec::new();

        tracing::debug!("update::Processing messages for {} ", self.username);
        // Go through the list of messages and process or store them.
        let identity = {
            let identity = self.identity.borrow();
            identity.identity().to_vec()
        };
        for message in self.backend.recv_msgs(&identity)?.drain(..) {
            //let sender = from_utf8(message.sender()).unwrap().split("#").next().unwrap().to_string();
            self.process_in_message(message)?;
        }
        tracing::debug!("update::Processing messages done");

        self.update_clients()?;
        Ok(messages_out)
    }

    /// Create a group with the given name.
    pub fn create_group(&mut self, group_name: String) -> Result<(), String> {
        self.subscribe(group_name.clone())?;
        tracing::debug!("Creates group {}", group_name);
        let group_id = group_name.as_bytes();
        let mut group_aad = group_id.to_vec();
        group_aad.extend(b" AAD");

        let group_config = MlsGroupCreateConfig::builder()
            .use_ratchet_tree_extension(false)
            //.max_past_epochs(0)
            .build();

        let now = ThreadTime::now();
        let mut mls_group = MlsGroup::new_with_group_id(
            &self.crypto,
            &self.identity.borrow().signer,
            &group_config,
            GroupId::from_slice(group_id),
            self.identity.borrow().credential_with_key.clone(),
        )
        .expect("Failed to create MlsGroup");
        let elapsed = now.elapsed().as_micros();

        mls_group.set_aad(group_aad);

        let timestamp = Utc::now().timestamp_nanos_opt().unwrap();
        let epoch = mls_group.epoch().as_u64();

        let group = Group {
            group_name: group_name.clone(),
            conversation: Conversation::default(),
            mls_group: RefCell::new(mls_group),
        };

        if self.groups.borrow().contains_key(&group_name) {
            panic!("Group '{}' existed already", group_name);
        }
        self.active.insert(group_name.clone(), (true, 0));

        self._publish_group_info(&group.mls_group.borrow(), None)?;
        self.groups.borrow_mut().insert(group_name.clone(), group);


        let transcript_hash = self.groups.borrow().get(&group_name).unwrap()
            .mls_group.borrow().transcript_hash();

        
        match &mut self.agent_type {
            AgentType::Independent => {}
            AgentType::Orchestrator(params, _, ref mut orchestrators) => {
                let mut orchestrator = Orchestrator::new(self.username.clone(), params.clone(), Vec::new());

                let next_action = orchestrator.process_commit(&transcript_hash, 1, vec![self.username.clone()]);

                orchestrators.insert(group_name.clone(), orchestrator);

                // next_action should be Commit -- if not, something is wrong
                if next_action == NextAction::Commit {
                    self.send_to_agent(group_name.clone())?;
                }
                else {
                    tracing::error!("After creating a group, expected NextAction::Commit, got {:?}", next_action);
                    unreachable!();
                }
            }
        }

        self.write_timestamp(ActionRecord {
            epoch_change: EpochChange{
                timestamp, epoch
            },
            group_name: group_name.clone(),
            action: CGKAAction::Create,
            elapsed_time: elapsed,
            num_users: 1,
        });

        Ok(())
    }

    /// Invite user with the given name to the group.
    pub fn invite(&mut self, name: String, group_name: String) -> Result<EpochChange, String> {

        // First we need to get the key package for {id} from the DS.
        let contact = match self.contacts.values().find(|c| c.username == name) {
            Some(v) => v,
            None => return Err(format!("No contact with name {name} known.")),
        };

        let joiner_key_package = self.consume_key_package(&contact)?;

        let now = ThreadTime::now();
        // Build a proposal with this key package and do the MLS bits.
        let (out_messages, welcome, new_group_info, crypto_time) = {
            let mut groups = self.groups.borrow_mut();
            let group = match groups.get_mut(&group_name) {
                Some(g) => g,
                None => return Err(format!("No group with name {group_name} known.")),
            };

            let result = black_box(group
                .mls_group
                .borrow_mut()
                .add_members_deep(
                //.add_members(
                    &self.crypto,
                    &self.identity.borrow().signer,
                    &[joiner_key_package.into()],
                )
                .map_err(|e| format!("Failed to add member to group - {e}"))?);


            result
        };

        let mut elapsed = now.elapsed().as_micros();

        let (export_time, merge_time) = {
            let mut groups = self.groups.borrow_mut();
            let group = match groups.get_mut(&group_name) {
                Some(g) => g,
                None => return Err(format!("No group with name {group_name} known.")),
            };

            let now = ThreadTime::now();
            let _tree = black_box(group.mls_group.borrow().export_ratchet_tree());
            let export_time = now.elapsed().as_micros();

            let merge_time = group.mls_group.borrow_mut().merge_pending_commit(self.crypto())
                .map_err(|e| format!("Error merging commit; {e}"))?;

            if let Some(ref merge_time) = merge_time {
                elapsed += merge_time.total;
            }

            (export_time, merge_time)
        };

        let stored_welcome = StoredWelcome {
            welcome: welcome,
            recipients: vec![name.clone()]
        };

        let size = out_messages.tls_serialized_len();
        // Second, process the invitation on our end.
        let epoch_change = self.post_process_commit(
            group_name.clone(),
            out_messages,
            new_group_info,
            Some(stored_welcome),
            //CGKAAction::Invite(name, size),
            CGKAAction::DeepCommit(size, crypto_time, export_time, merge_time.unwrap_or_default()),
            elapsed
        )?;

        Ok(epoch_change)
    }

    pub fn propose(&mut self, action: CGKAAction, group_name: String) -> Result<(), String> {

        let proposal = {
            let groups = self.groups.borrow();
            let group = match groups.get(&group_name) {
                Some(g) => g,
                None => return Err("Unknown group".to_string()),
            };

            match action.clone() {
                CGKAAction::Update(_) => {
                    let leaf_node_parameters = LeafNodeParameters::default();
                    Propose::Update(leaf_node_parameters)
                }
                CGKAAction::Invite(user, _,) => {
                    for prop in group.mls_group.borrow_mut().pending_proposals() {
                        match prop.proposal() {
                            Proposal::Add(p) => {
                                if user.eq(from_utf8(p.key_package().leaf_node().credential().identity()).unwrap()) {
                                    return Err("There is already a pending proposal to add this user.".to_string());
                                }
                            }
                            _ => {}
                        }
                    }

                    let contact = match self.contacts.values().find(|c| c.username == user) {
                        Some(v) => v,
                        None => return Err(format!("No contact with name {user} known.")),
                    };

                    // Reclaim a key package from the server
                    let joiner_key_package = self.consume_key_package(&contact)?;
                    
                    Propose::Add(joiner_key_package.into())
                }
                CGKAAction::Remove(user, _) => {
                    Propose::Remove(self.find_member_index(user, group)?.u32())
                }
                _ => { unreachable!() }
            }
        };

        let now = ThreadTime::now();
        let (out_message, _) = {
            let groups = self.groups.borrow();
            let group = match groups.get(&group_name) {
                Some(g) => g,
                None => return Err("Unknown group".to_string()),
            };

            let result = group
                .mls_group
                .borrow_mut()
                .propose(&self.crypto, &self.identity.borrow().signer, proposal, ProposalOrRefType::Proposal)
                .map_err(|e| format!("{e}"))?;

            result
        };

        let elapsed = now.elapsed().as_micros();

        let size = out_message.tls_serialized_len();

        let action = match action {
            CGKAAction::Update(_) => CGKAAction::Update(size),
            CGKAAction::Invite(user, _) => CGKAAction::Invite(user, size),
            CGKAAction::Remove(user, _) => CGKAAction::Remove(user, size),
            _ => {unreachable!()}
        };

        let _epoch_change = self.post_process_commit(
            group_name.clone(),
            out_message,
            None,
            None,
            CGKAAction::Propose(Box::new(action)),
            elapsed
        )?;
        Ok(())
    }

    pub fn pending_proposals(&mut self, group_name: String) -> Result<usize, String> {
        let invalid_proposals = self.invalid_proposals(group_name.clone())?;

        let groups = self.groups.borrow();
        let group = match groups.get(&group_name) {
            Some(g) => g,
            None => return Err("Unknown group".to_string()),
        };

        let num = group.mls_group.borrow().pending_proposals().count() - invalid_proposals.len();

        Ok(num)
    }

    fn invalid_proposals(&self, group_name: String) -> Result<Vec<QueuedProposal>, String> {
        let groups = self.groups.borrow();
        let group = match groups.get(&group_name) {
            Some(g) => g,
            None => return Err("Unknown group".to_string()),
        };

        let invalid_proposals = {
            let mls_group = group.mls_group.borrow();
            mls_group.pending_proposals().cloned().filter(
                |p| {
                    if let Proposal::Remove(prop) = p.proposal() {
                        if prop.removed() == mls_group.own_leaf_index() {
                            return true;
                        }
                    }
                    return false;
                }
            ).collect::<Vec<QueuedProposal>>()
        };

        tracing::debug!("INVALID PROPOSALS: {:?}", invalid_proposals);

        Ok(invalid_proposals)
    }

    pub fn commit_to_proposals(&mut self, group_name: String, amount: usize) -> Result<EpochChange, String> {

        let ((out_message, welcome, new_group_info), proposals_to_commit, elapsed) = {
            let invalid_proposals = self.invalid_proposals(group_name.clone())?;

            let groups = self.groups.borrow();
            let group = match groups.get(&group_name) {
                Some(g) => g,
                None => return Err("Unknown group".to_string()),
            };

            for prop in invalid_proposals {
                group.mls_group.borrow_mut().remove_pending_proposal(
                    self.crypto.storage(),
                    &prop.proposal_reference()
                )
                    .map_err(|e| format!("Failed to remove proposal: {:?}", e))?;
            }
            let proposals_to_remove = {
                let pending_proposals = group.mls_group.borrow_mut().pending_proposals().cloned().collect::<Vec<QueuedProposal>>();
                let amount_to_remove = pending_proposals.len() - amount;

                pending_proposals.choose_multiple(
                    &mut rand::thread_rng(),
                    amount_to_remove
                ).cloned().collect::<Vec<QueuedProposal>>()
            };
            for prop in proposals_to_remove {
                group.mls_group.borrow_mut().remove_pending_proposal(
                    self.crypto.storage(),
                    &prop.proposal_reference()
                )
                    .map_err(|e| format!("Failed to remove proposal: {:?}", e))?;
            }

            let proposals_to_commit = {
                let mls_group = group.mls_group.borrow();
                
                mls_group.pending_proposals().map(|p| {
                    match p.proposal() {
                        Proposal::Add(add_proposal) => {
                            let new_user = from_utf8(
                                add_proposal.key_package().leaf_node().credential().identity()
                            ).unwrap().to_string();
                            CGKAAction::Invite(new_user, 0)
                        },
                        Proposal::Remove(remove_proposal) => {
                            let removed_user = from_utf8(
                                mls_group.member_at(remove_proposal.removed()).unwrap()
                                    .credential.identity()
                            ).unwrap().to_string();
                            
                            CGKAAction::Remove(removed_user, 0)
                        }
                        Proposal::Update(_) => {
                            CGKAAction::Update(0)
                        }
                        _ => {
                            unreachable!()
                        }

                    }
                }).collect::<Vec<CGKAAction>>()
            };

            let now = ThreadTime::now();
            let result = group
                .mls_group
                .borrow_mut()
                .commit_to_pending_proposals(
                    &self.crypto,
                    &self.identity.borrow().signer,
                )
                .map_err(|e| format!("{e}"))?;

            let elapsed = now.elapsed().as_micros();
            (result, proposals_to_commit, elapsed)
        };

        let new_users = proposals_to_commit.iter().filter_map(|p| {
            match p {
                CGKAAction::Invite(user, _) => Some(user.clone()),
                _ => None,
            }
        }).collect::<Vec<String>>();

        let stored_welcome = match welcome {
            Some(welcome) => Some(StoredWelcome {
                welcome,
                recipients: new_users
            }),
            None => None,
        };

        let size = out_message.tls_serialized_len();

        let epoch_change = self.post_process_commit(
            group_name.clone(),
            out_message,
            new_group_info,
            stored_welcome,
            CGKAAction::Commit(proposals_to_commit, size),
            elapsed
        )?;
        Ok(epoch_change)
    }
    pub fn update_state(&mut self, group_name: String) -> Result<EpochChange, String> {

        let now = ThreadTime::now();

        // Get the group ID
        let (update_message, _welcome, new_group_info) = {
            let mut groups = self.groups.borrow_mut();
            let group = match groups.get_mut(&group_name) {
                Some(g) => g,
                None => return Err(format!("No group with name {group_name} known.")),
            };

            // Remove operation on the mls group
            let result = group
                .mls_group
                .borrow_mut()
                .self_update(&self.crypto, &self.identity.borrow().signer, LeafNodeParameters::default())
                .map_err(|e| format!("Failed to self update - {e}"))?;

            (result.commit().clone(), result.welcome().cloned(), result.group_info().cloned())
        };
        let elapsed = now.elapsed().as_micros();

        let size = update_message.tls_serialized_len();
        let epoch_change = self.post_process_commit(
            group_name.clone(),
            update_message,
            new_group_info,
            None,
            CGKAAction::Update(size),
            elapsed
        )?;
        Ok(epoch_change)
    }

    /// Remove user with the given name from the group.
    pub fn remove(&mut self, name: String, group_name: String) -> Result<EpochChange, String> {

        let now = ThreadTime::now();
        let (remove_message, _welcome, new_group_info) = {
            let mut groups = self.groups.borrow_mut();
            let group = match groups.get_mut(&group_name) {
                Some(g) => g,
                None => return Err(format!("No group with name {group_name} known.")),
            };

            // Get the client leaf index

            let leaf_index = match self.find_member_index(name.clone(), group) {
                Ok(l) => l,
                Err(e) => return Err(e),
            };

            // Remove operation on the mls group
            let result = group
                .mls_group
                .borrow_mut()
                .remove_members(&self.crypto, &self.identity.borrow().signer, &[leaf_index])
                .map_err(|e| format!("Failed to remove member from group - {e}"))?;

            result
        };
        let elapsed = now.elapsed().as_micros();

        let size = remove_message.tls_serialized_len();
        let epoch_change = self.post_process_commit(
            group_name.clone(),
            remove_message,
            new_group_info,
            None,
            CGKAAction::Remove(name.clone(), size),
            elapsed
        )?;

        Ok(epoch_change)
    }

    pub fn publish_group_info(&self, group_name: String) -> Result<(), String> {
        let groups = self.groups.borrow();
        let group = match groups.get(&group_name) {
            Some(g) => g,
            None => return Err("Unknown group".to_string()),
        };

        let result = self._publish_group_info(&group.mls_group.borrow(), None);

        result
    }
    
    fn _publish_group_info(&self, group: &MlsGroup, group_info: Option<GroupInfoAndTree>) -> Result<(), String> {
        let group_info = match group_info {
            Some(gi) => gi.into(),
            None => {
                
                let group_info = group.export_group_info(&self.crypto, &self.identity.borrow().signer, true)
                    .map_err(|e| format!("Failed to export group info - {e}"))?
                    .into_verifiable_group_info().unwrap();

                let ratchet_tree = group.export_ratchet_tree();
                GroupInfoAndTree {
                    group_info,
                    ratchet_tree: Some(ratchet_tree.into())
                }
            }
        };

        let group_name = from_utf8(group.group_id().as_slice()).unwrap().to_string();

        // Finally, send GroupInfo.
        tracing::info!("Sending new group info");
        match self.ds {
            DeliveryService::Request | DeliveryService::PubSubMQTT(_) | DeliveryService::GossipSub(_, Directory::Server) => {
                self.backend
                    .publish_group_info(self.username.clone(), group_name, group_info)
            }
            
            DeliveryService::GossipSub(ref broker, Directory::Kademlia) => {
                    let broker = broker.lock().unwrap();
                    broker.publish_group_info(group_name, group_info)?;
                    drop(broker);
                    Ok(())
            }
        }
    }

    pub fn external_join(&mut self, group_name: String, group_info: GroupInfoAndTree) -> Result<EpochChange, String> {

        tracing::info!("Joining group {}", group_name);
        self.subscribe(group_name.clone()).unwrap();

        let GroupInfoAndTree { group_info, ratchet_tree } = group_info;
        // First we need to get the group info for {id} from the DS.
        let group_id = group_name.as_bytes();
        let mut group_aad = group_id.to_vec();
        group_aad.extend(b" AAD");
        let gi_size = group_info.tls_serialized_len();

        let group_config = MlsGroupJoinConfig::builder()
            .use_ratchet_tree_extension(false)
            //.max_past_epochs(0)
            .build();

        let now = ThreadTime::now();

        let (new_mls_group, out_messages, new_group_info) = MlsGroup::join_by_external_commit(
            &self.crypto,
            &self.identity.borrow().signer,
            ratchet_tree,
            group_info,
            &group_config,
            None, None,
            group_aad.as_slice(),
            self.identity.borrow().credential_with_key.clone()
        ).expect("Error creating external join message");

        let elapsed = now.elapsed().as_micros();

        let new_group = Group {
            group_name: group_name.clone(),
            conversation: Conversation::default(),
            mls_group: RefCell::new(new_mls_group),
        };

        self.active.insert(group_name.clone(), (true, 0));

        {
            let mut groups = self.groups.borrow_mut();
            groups.insert(group_name.clone(), new_group);
        }

        let commit_size = out_messages.tls_serialized_len();

        let epoch_change = self.post_process_commit(
            group_name.clone(),
            out_messages,
            new_group_info,
            None,
            CGKAAction::Join(commit_size, gi_size),
            elapsed
        )?;

        //let new_group = groups.get_mut(&group_name).unwrap();

        Ok(epoch_change)
    }

    fn post_process_commit(
        &mut self,
        group_name: String,
        msg_out: MlsMessageOut,
        group_info: Option<GroupInfo>,
        welcome: Option<StoredWelcome>,
        action: CGKAAction,
        elapsed_time: u128
    ) -> Result<EpochChange, String> {
        tracing::debug!("Post processing commit for group {}", group_name);

        let (timestamp, epoch) = {
            let groups = self.groups.borrow();
            let group = groups.get(&group_name).expect("Group should be present after mutable borrow.");
            let mls_group = group.mls_group.borrow_mut();
    
            /*if !matches!(action, CGKAAction::Propose(..)) {
                // Sleep 1 sec to give time to new users to subscribe
                if !matches!(self.ds, DeliveryService::GossipSub(..)) {
                    //thread::sleep(Duration::from_secs(1));
                }
            }*/

            let timestamp = Utc::now().timestamp_nanos_opt().unwrap();
            let epoch = mls_group.epoch().as_u64();

            let action_record = ActionRecord {
                action: action.clone(),
                group_name: group_name.clone(),
                epoch_change: EpochChange {
                    timestamp,
                    epoch
                },
                elapsed_time,
                num_users: mls_group.members().count(),
            };

            let group_recipients = match self.ds {
                DeliveryService::Request => {self.mls_recipients(&mls_group)}
                _ => {Vec::new()}
            };

            self.send_to_group(group_name.clone(), msg_out, &group_recipients)?;
            self.pending_commits.insert(group_name.clone(), (group_info.clone(), welcome, action_record));

            (timestamp, epoch)
        };

        if let AgentType::Orchestrator(_, _, _) = &self.agent_type {
            //TEST: Confirm commit immediately
            self.confirm_commit(group_name.clone())?;
        }

        Ok(EpochChange {
            timestamp,
            epoch
        })
    }

    /// Join a group with the provided welcome message.
    fn join_group(&self, welcome: Welcome, ratchet_tree: RatchetTreeIn) -> Result<(String, EpochChange, u128), String> {

        tracing::debug!("Joining group ...");

        let mut ident = self.identity.borrow_mut();
        for secret in welcome.secrets().iter() {
            let key_package_hash = &secret.new_member();
            if ident.kp.contains_key(key_package_hash.as_slice()) {
                ident.kp.remove(key_package_hash.as_slice());
            }
        }

        
        let group_config = MlsGroupJoinConfig::builder()
            .use_ratchet_tree_extension(false)
            //.max_past_epochs(0)
            .build();

        let now = ThreadTime::now();

        let staged_welcome = StagedWelcome::new_from_welcome(&self.crypto, &group_config, welcome, Some(ratchet_tree))
            .map_err(|e| format!("Error staging Welcome: {:?}", e))?;

        let mut mls_group = staged_welcome.into_group(&self.crypto)
            .map_err(|e| format!("Error joining group: {:?}", e))?;

        let elapsed = now.elapsed().as_micros();

        let timestamp = Utc::now().timestamp_nanos_opt().unwrap();
        let epoch = mls_group.epoch().as_u64();

        let group_id = mls_group.group_id().to_vec();

        let group_name = String::from_utf8(group_id.clone()).unwrap();
        let group_aad = group_name.clone() + " AAD";

        mls_group.set_aad(group_aad.as_bytes().to_vec());


        tracing::info!("JOINED GROUP {}. Epoch: {})", group_name, mls_group.epoch().as_u64());

        let group = Group {
            group_name: group_name.clone(),
            conversation: Conversation::default(),
            mls_group: RefCell::new(mls_group),
        };

        self.groups.borrow_mut().insert(group_name.clone(), group);



        Ok((
            group_name,
            EpochChange {
                timestamp,
                epoch
            },
            elapsed
        ))
    }

    fn send_to_group(&self, group_name: String, message_out: MlsMessageOut, recipients: &Vec<Vec<u8>>) -> Result<(), String> {

        let msg = GroupMessage::new(message_out.into(),self.username.clone().into(), None, recipients, None);
        match self.ds {
            DeliveryService::Request => {
                self.backend.send_msg(&msg)
            }
            DeliveryService::PubSubMQTT(ref broker)  => {
                let broker = broker.lock().unwrap();
                broker.send_msg(&msg, group_name.clone())
            }
            DeliveryService::GossipSub(ref broker, _) => {
                let broker = broker.lock().unwrap();
                broker.send_msg(&msg, group_name.clone())
            }
        }
    }

    fn send_ack(&self, group_name: String) {
        let ack = WelcomeAcknowledgement {
            sender: self.username.clone(),
            group: group_name,
        };

        match self.ds {
            DeliveryService::Request => {
                todo!();
            }
            DeliveryService::PubSubMQTT(ref broker)  => {
                let broker = broker.lock().unwrap();
                broker.send_ack(&ack).unwrap();
                drop(broker);
            }
            DeliveryService::GossipSub(ref broker, _) => {
                let broker = broker.lock().unwrap();
                broker.send_ack(&ack).unwrap();
                drop(broker);
            }
        }
    }

    pub fn group_list(&self) -> Vec<String> {
        self.groups.borrow().keys().cloned().collect()
    }

    pub(crate) fn confirm_commit(&mut self, group_name: String) -> Result<(), String>{
        // Get and remove from the pending commits
        let (group_info, stored_welcome, action_record) = match self.pending_commits.remove(&group_name) {
            Some(gi) => gi,
            None => return Ok(()),
        };

        let merge_time = {
            let groups = self.groups.get_mut();
            let group = match groups.get_mut(&group_name) {
                Some(g) => g,
                None => return Err(format!("No group with name {group_name} known.")),
            };

            let mut mls_group = group.mls_group.borrow_mut();
            
            mls_group
                .merge_pending_commit(&self.crypto)
                .expect("error merging pending commit")
            
        };

        let ratchet_tree = {
            let groups = self.groups.borrow();
            let group = groups.get(&group_name).unwrap();
            let tree = group.mls_group.borrow().export_ratchet_tree();

            tree
        };

        let new_members = if let Some(stored_welcome) = stored_welcome {
            let StoredWelcome {welcome, recipients} = stored_welcome;
            let new_members: Vec<String> = recipients.clone();

            let encoded_recipients: Vec<Vec<u8>> = recipients.clone().into_iter()
                                        .map(|s| s.as_bytes().to_vec())
                                        .collect();

            let active_members = match &self.agent_type {
                AgentType::Independent => None,
                AgentType::Orchestrator(_, _, orchestrators) => {
                    let orchestrator = orchestrators.get(&group_name).unwrap();
                    let active_members = orchestrator.known_members.clone();

                    let active_members = active_members.into_iter()
                    .map(|s| s.as_bytes().to_vec())
                                                .collect::<Vec<Vec<u8>>>();
                    
                    Some(active_members)    

                }
            };

            let msg = GroupMessage::new(welcome.clone().into(),self.username.clone().into(), Some(ratchet_tree.clone().into()), &encoded_recipients, active_members);
            for recipient in recipients {
                match self.ds {
                    DeliveryService::Request => {
                        tracing::trace!("Sending welcome");
                        self.backend
                            .send_welcome(&msg)?;
                    }
                    DeliveryService::PubSubMQTT(ref broker) => {
                        let broker = broker.lock().unwrap();
                        broker.send_welcome(&msg, recipient.clone())?;
                        drop(broker)
                    }
                    DeliveryService::GossipSub(ref broker, _) => {
                        let broker = broker.lock().unwrap();
                        broker.send_welcome(&msg, recipient.clone())?;
                        drop(broker)
                    }
                }
            }
            new_members

        } else {vec![]};

        let edited_record = match action_record.clone().action {
            CGKAAction::Propose(_) => {action_record},
            _ => {

                let removed_members = match action_record.action {
                    CGKAAction::Remove(ref user, _) => vec![user.clone()],
                    _ => vec![]
                };

                let groups = self.groups.borrow();
                let group = groups.get(&group_name).unwrap();
                let mls_group = group.mls_group.borrow();

                let group_info_and_tree = match group_info {
                    Some(gi) => Some(GroupInfoAndTree::new(
                        gi.into(),
                        Some(ratchet_tree.into())
                    )),
                    None => None,

                };
                self._publish_group_info(&mls_group, group_info_and_tree)?;

                match &mut self.agent_type {
                    AgentType::Independent => {}
                    AgentType::Orchestrator(_, _, ref mut orchestrators) => {
                        let transcript_hash: Vec<u8> = mls_group.transcript_hash();
                        let total_members = mls_group.members().count();

                        let orchestrator = orchestrators.get_mut(&group_name).unwrap();
                        orchestrator.remove_members(removed_members);
                        let next_action = orchestrator.process_commit(
                            &transcript_hash,
                            total_members,
                            new_members
                        );

                        match next_action {
                            NextAction::Commit => {
                                self.send_to_agent(group_name.clone())?;
                            }
                            _ => {}
                        }
                    }
                };

                let additional_commit_time = if let Some(merge_time) = merge_time {
                    merge_time.total
                } else {
                    0
                };

                // "Epoch" and "num_users" were calculated while the commit was pending
                ActionRecord {
                    epoch_change: EpochChange {
                        epoch: action_record.epoch_change.epoch + 1,
                        ..action_record.epoch_change.clone()
                    },
                    num_users: mls_group.members().count(),
                    elapsed_time: action_record.elapsed_time + additional_commit_time,
                    ..action_record
                }
            }
        };

        self.write_timestamp(edited_record);

        Ok(())

    }

    /*fn undo_commit(&mut self, mls_group: &MlsGroup, action_record: ActionRecord) -> Result<(), String> {
        tracing::info!("undoing commit");

        match action_record.action {

            CGKAAction::Commit(..) | CGKAAction::Join(..) | CGKAAction::Invite(..) | CGKAAction::Remove(..) | CGKAAction::Update(..) => {
                let attempted_action = ActionRecord {
                    epoch_change: EpochChange {
                        epoch: action_record.epoch_change.epoch + 1,
                        ..action_record.epoch_change.clone()
                    },
                    action: CGKAAction::CommitAttempt(Box::new(action_record.action.clone())),
                    ..action_record
                };
                self.write_timestamp(attempted_action, false);
            },
            // Only commits can be undone - should not be possible to reach this function with other actions
            _ => {unreachable!()}
        };

        self._publish_group_info(mls_group, None)

    }*/

    pub fn is_authorised(&self, group_name: String, authorization_policy: AuthorizationPolicy) -> Result<bool, String> {

        // If the user is desync, not authorised
        if !self.active.get(&group_name).unwrap_or(&(false, 0)).0 {
            tracing::info!("Desync-ed, cannot issue updates");
            return Ok(false);
        }

        let groups = self.groups.borrow();
        let group = match groups.get(&group_name) {
            Some(g) => g,
            None => return Err("Unknown group".to_string()),
        };

        let mls_group = group.mls_group.borrow();

        let leaf_node_index = mls_group.own_leaf_index().u32();

        match authorization_policy {
            AuthorizationPolicy::Random => Ok(true),
            AuthorizationPolicy::First => {
                Ok(leaf_node_index == 0)
            }
            AuthorizationPolicy::Last => {
                Ok(leaf_node_index == mls_group.members().count() as u32 - 1)
            }
        }
    }

    pub fn process_ack(&mut self, ack: WelcomeAcknowledgement) -> Result<(), String> {
        let WelcomeAcknowledgement {sender, group} = ack;

        if sender == self.username {
            tracing::debug!("Ignoring own ack");
            return Ok(());
        }

        match &mut self.agent_type {
            AgentType::Independent => {},
            AgentType::Orchestrator(_, _, ref mut orchestrators) => {
                let orchestrator = orchestrators.get_mut(&group).unwrap();
                let next_action = orchestrator.process_ack();

                match next_action {
                    NextAction::Commit => {
                        self.send_to_agent(group)?;
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    pub fn send_to_agent(&self, msg: String) -> Result<(), String> {

        match &self.agent_type {
            AgentType::Independent => unreachable!("Independent agents do not communicate with agent."),
            AgentType::Orchestrator(_, sender, _) => {
                let tx = sender.clone();

                thread::spawn(move || {
                    block_on(async {
                        tx.send(msg).await.map_err(|e| format!("Error sending to agent: {e}"))
                    })
                }).join().map_err(|e| format!("Error joining thread: {:?}", e))?
            }
        }
    }

    pub fn write_timestamp(&mut self, action_record: ActionRecord) {
        
        self.file_buffer.push(action_record.clone());

        if self.file_buffer.len() >= FILE_BUFFER_SIZE || matches!(action_record.action, CGKAAction::Welcome(_, _)) {
            self.flush_logs();
        }
    }

    fn flush_logs(&mut self) {
        let to_write = self.file_buffer.drain(..).map(
            |item| {
                let ActionRecord {group_name, action, epoch_change, elapsed_time, num_users} = item;

                let mut to_write = group_name.clone();
                to_write.push_str(" ");
                to_write.push_str(epoch_change.epoch.to_string().as_str());
                to_write.push_str(" ");
                to_write.push_str(num_users.to_string().as_str());
                to_write.push_str(" ");
                to_write.push_str(&self.username);
                to_write.push_str(" ");
                to_write.push_str(format!("{}", action).as_str());
                to_write.push_str(" ");
                to_write.push_str(epoch_change.timestamp.to_string().as_str());
                to_write.push_str(" ");
                to_write.push_str(elapsed_time.to_string().as_str());

                to_write
            }
        ).collect::<Vec<String>>().join("\n");

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(format!("logs/{}.txt", self.username.clone()))
            .unwrap();

        if let Err(e) = writeln!(file, "{}", to_write) {
            eprintln!("Couldn't write to file: {}", e);
        }
    }
}
