use std::sync::{Arc, Mutex};
use std::{thread};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::fs::OpenOptions;
use std::time::Duration;
use rand::Rng;
use rumqttc::{AsyncClient, MqttOptions};
use url::Url;
use std::io::Write;
use rand::distributions::Alphanumeric;
use crate::pubsub::mqtt_updater::MqttUpdater;
use crate::pubsub::Broker;
use crate::pubsub::gossipsub_broker::GossipSubBroker;
use crate::pubsub::gossipsub_updater::{GossipSubQueueMessage, GossipSubUpdater};
use crate::pubsub::mqtt_broker::MqttBroker;
use crate::user::{DeliveryService, EpochChange, User};
use crate::user_parameters::{Paradigm, DSType, UserParameters};

#[derive(Debug, Clone)]
pub struct ActionRecord {
    pub group_name: String,
    pub action: CGKAAction,
    pub epoch_change: EpochChange,
    pub elapsed_time: u128
}

#[derive(Debug, Clone)]
pub enum CGKAAction {
    Propose(Box<CGKAAction>),
    Commit(usize, usize),
    Join(usize, usize),
    Update(usize),
    Invite(String, usize),
    Remove(String, usize),
    Process(String),
    StoreProp(String),
    Welcome(String, usize),
    Create,

    SetRecord,
    RecvRecord,
}

pub struct ActionWithTime {
    pub action: ActionRecord,
}

impl Display for CGKAAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CGKAAction::Propose(action) => {write!(f, "Propose {}", action)},
            CGKAAction::Commit(proposals, size) => {write!(f, "Commit {proposals} {size}")}
            CGKAAction::Join(c_size, gi_size) => {write!(f, "Join {c_size} {gi_size}")}
            CGKAAction::Update(size) => {write!(f, "Update {size}")}
            CGKAAction::Invite(user, size) => {write!(f, "Invite {user} {size}")}
            CGKAAction::Remove(user, size) => {write!(f, "Remove {user} {size}")}
            CGKAAction::Process(user) => {
                match user.as_str() {
                    "" => write!(f, "Process"),
                    _ => write!(f, "Process {user}")
                }
            }
            CGKAAction::Welcome(user, size) => {
                match user.as_str() {
                    "" => write!(f, "Welcome"),
                    _ => write!(f, "Welcome {user} {size}")
                }
            }
            CGKAAction::StoreProp(user) => {
                match user.as_str() {
                    "" => write!(f, "StoreProp"),
                    _ => write!(f, "StoreProp {user}")
                }
            }
            CGKAAction::Create => {write!(f, "Create")}
            CGKAAction::SetRecord => {write!(f, "SetRecord")}
            CGKAAction::RecvRecord => {write!(f, "RecvRecord")}
        }
    }
}

pub struct ClientAgent {
    username: String,
    parameters: UserParameters
}

impl ClientAgent {
    pub fn new(
        name: String,
        parameters: UserParameters,
    ) -> Self {
        ClientAgent {
            username: name,
            parameters
        }
    }


    pub fn create_user_from_ds(user_parameters: UserParameters, replicas: i64, username: String) -> Result<HashMap<String, Arc<Mutex<User>>>, String> {
        let mut users = HashMap::new();
         match user_parameters.delivery_service {
            DSType::Request => {
                for i in 0..replicas {
                    let name = format!("{}_{}",username, i);
                    let user = User::new(name.clone(), user_parameters.server_url.clone(), DeliveryService::Request);
                    let user = Arc::new(Mutex::new(user));

                    users.insert(name, user.clone());
                }
                Ok(users)
            },
            DSType::PubSubMQTT(url_str) => {
                for i in 0..replicas {
                    let name = format!("{}_{}",username, i);

                    let url = Url::parse(&url_str)
                        .map_err(|e| format!("Error parsing URL: {:?}", e))?;
                    
                    let mut mqtt_options = MqttOptions::new(name.clone(), url.host_str().unwrap(), url.port().unwrap());
                    mqtt_options.set_keep_alive(Duration::from_secs(10));
                    mqtt_options.set_max_packet_size(1024 * 1024, 1024 * 1024);

                    let (async_client, event_loop) = AsyncClient::new(mqtt_options, 200);

                    let mut broker = MqttBroker::new_from_client(async_client);
                    //broker.subscribe_welcome(username.clone())?;

                    let broker = Arc::new(Mutex::new(broker));
                    let ds = DeliveryService::PubSubMQTT(Arc::clone(&broker));

                    let user = User::new(name.clone(), user_parameters.server_url.clone(), ds.clone());

                    let user = Arc::new(Mutex::new(user));

                    let user_thread = Arc::clone(&user);
                    let name_thread = name.clone();
                    thread::spawn(move || {
                        let mut mqtt_updater = MqttUpdater::new(name_thread, user_thread, event_loop);
                        mqtt_updater.run();
                    });

                    users.insert(name, user.clone());
                }


                Ok(users)
            }
            DSType::GossipSub(config) => {
                let (tx, rx) = tokio::sync::mpsc::channel::<GossipSubQueueMessage>(100);

                for i in 0..replicas {
                    let name = format!("{}_{}",username, i);

                    let mut broker = GossipSubBroker::new(name.clone(), tx.clone());

                    let broker = Arc::new(Mutex::new(broker));
                    let ds = DeliveryService::GossipSub(Arc::clone(&broker), config.directory.clone());

                    let user = User::new(name.clone(), user_parameters.server_url.clone(), ds.clone());
                    let user = Arc::new(Mutex::new(user));
                    //log::info!("{} -> Creating GossipSubUpdater", self.username);

                    users.insert(name, user.clone());
                }

                let users_thread: HashMap<String, Arc<Mutex<User>>> = users
                    .iter().map(|(name, u)| (name.clone(), Arc::clone(u))).collect();

                let mut gossipsub_updater = GossipSubUpdater::new(username.clone(), users_thread, config.clone(), rx);
                //let contact_info = gossipsub_updater.init(address_str).clone();

                thread::spawn(move || {
                    gossipsub_updater.run();
                });

                log::info!("{} -> Bootstrapping P2P network...", username);
                thread::sleep(Duration::from_secs(60));

                Ok(users)
            }
        }
    }

    pub fn run(&self, user: Arc<Mutex<User>>) {
        log::info!("Starting user {}", self.username);

        let mut rng = rand::thread_rng();

        {
            let mut user = user.lock().unwrap();
            user.subscribe_welcome().unwrap();
            match user.register() {
                Ok(_) => {}
                Err(e) => {
                    if e.contains("409") {
                        log::info!("{} -> User is in use", self.username);
                        panic!("User is in use");
                    }
                    panic!("Error registering user: {}", e);
                }
            }
            user.create_kp().unwrap();
            drop(user);
        }

        loop {

            let millis_to_sleep = rng.gen_range(self.parameters.sleep_millis_min..self.parameters.sleep_millis_max) as u64;
            thread::sleep(Duration::from_millis(millis_to_sleep));
            {
                let mut user = user.lock().unwrap();
                let groups_of_user = user.list_of_groups();

                if self.parameters.delivery_service == DSType::Request {
                    //log::info!("Requesting updates for {}", self.username);
                    user.update().expect("Error updating group");
                }

                for group_name in user.group_list() {

                    match user.is_authorised(group_name.clone(), self.parameters.auth_policy.clone()) {
                        Ok(is_auth) => {
                            if !is_auth {
                                continue;
                            }
                        }

                        Err(e) => {log::error!("Error checking authorisation: {:?}", e); continue;}
                    }

                    if rng.gen::<f64>() < self.scale(self.parameters.issue_update_chance, &user, group_name.clone()) {
                        // Issue an update
                        let mut rng = rand::thread_rng();
                        let random_value: f64 = rng.gen_range(0.0..1.0);

                        if random_value < self.parameters.invite_chance {
                            match user.update_clients() {
                                Ok(_) => {}
                                Err(e) => { log::error!("Error updating clients: {}", e); }
                            };

                            //add random user from group
                            let candidates = user.not_members_of_group(group_name.clone());

                            log::info!("{}: {} candidates to invite", self.username, candidates.len());

                            if candidates.len() == 0 {
                                log::info!("{}: No candidates to invite", self.username);
                                continue;
                            }

                            let user_to_add = candidates[rng.gen_range(0..candidates.len())].clone();
                            match self.parameters.paradigm {
                                Paradigm::Commit => {
                                    //Mutex
                                    if let Err(_) = user.get_group_info(group_name.clone()) {
                                        continue;
                                    }

                                    log::info!("{} -> Inviting {} to {}", self.username, user_to_add, group_name);
                                    match user.invite(user_to_add.clone(), group_name.clone()) {
                                        Ok(_) => {},
                                        Err(error) => {
                                            log::error!("Error inviting user: {}", error);
                                            user.publish_group_info(group_name.clone()).unwrap();
                                        },
                                    }
                                    continue;
                                }
                                Paradigm::Propose => {
                                    log::info!("{} -> Proposing Invite {} to {}", self.username, user_to_add, group_name);
                                    let action = CGKAAction::Invite(user_to_add, 0);
                                    match user.propose(action, group_name.clone()) {
                                        Ok(_) => {},
                                        Err(error) => {
                                            log::error!("Error proposing Invite: {}", error);
                                        },
                                    }
                                }
                            }
                        }
                        else if random_value < (self.parameters.invite_chance + self.parameters.remove_chance) {

                            //remove random user from group
                            let members = user.members_of_group(group_name.clone());
                            if members.len() == 0 {
                                continue;
                            }

                            let user_to_remove = members[rng.gen_range(0..members.len())].clone();

                            match self.parameters.paradigm {
                                Paradigm::Commit => {
                                    //Mutex
                                    if let Err(_) = user.get_group_info(group_name.clone()) {continue;}

                                    log::info!("{} -> Removing {} from {}", self.username, user_to_remove, group_name);
                                    match user.remove(user_to_remove.clone(), group_name.clone()) {
                                        Ok(_) => {},
                                        Err(error) => {
                                            log::error!("Error removing user: {}", error);
                                        },
                                    }
                                    continue;
                                }
                                Paradigm::Propose => {
                                    log::info!("{} -> Proposing Remove {} from {}", self.username, user_to_remove, group_name);
                                    let action = CGKAAction::Remove(user_to_remove, 0);
                                    match user.propose(action, group_name.clone()) {
                                        Ok(_) => {},
                                        Err(error) => {
                                            log::error!("Error proposing Remove: {}", error);
                                        },
                                    }
                                }
                            }
                        }
                        else {
                            match self.parameters.paradigm {
                                Paradigm::Commit => {
                                    //Mutex
                                    if let Err(_) = user.get_group_info(group_name.clone()) {continue;}

                                    log::info!("{} -> Updating in {}", self.username, group_name);
                                    match user.update_state(group_name.clone()) {
                                        Ok(_) => {},
                                        Err(error) => {
                                            log::error!("Error updating user: {}", error);
                                        },
                                    }
                                    continue;
                                }
                                Paradigm::Propose => {

                                    let action = CGKAAction::Update(0);
                                    log::info!("{} -> Proposing Update in {}", self.username, group_name);
                                    match user.propose(action, group_name.clone()) {
                                        Ok(_) => {},
                                        Err(error) => {
                                            log::error!("Error proposing user: {}", error);
                                        },
                                    }
                                }
                            }
                        }
                    }

                    if rng.gen::<f64>() < self.scale(self.parameters.issue_update_chance, &user, group_name.clone())
                        && self.parameters.paradigm == Paradigm::Propose {
                        // Commit to proposals
                        if user.pending_proposals(group_name.clone()).unwrap() <= self.parameters.proposals_per_commit {continue;}

                        if let Err(_) = user.get_group_info(group_name.clone()) {continue;}
                        log::info!("{} -> Committing to Proposals in {}", self.username, group_name);
                        match user.commit_to_proposals(group_name.clone(), self.parameters.proposals_per_commit) {
                            Ok(_) => {},
                            Err(error) => {
                                log::error!("Error committing to proposals: {}", error);
                                user.publish_group_info(group_name.clone()).unwrap();
                            },
                        }
                        continue;
                    }

                    if rng.gen::<f64>() < self.scale(self.parameters.message_chance, &user, group_name.clone()) {
                        // Send Application message
                        log::info!("{} -> Sending message in {}", self.username, group_name);
                        let length = rng.gen_range(
                            self.parameters.message_length_min..self.parameters.message_length_max
                        );

                        let message = (&mut rng).sample_iter(&Alphanumeric)
                            .take(length)
                            .map(char::from)
                            .collect();
                        match user.send_application_msg(message, group_name.clone()) {
                            Ok(()) => {}
                            Err(e) => {log::error!("Error sending message: {}", e);}
                        }
                    }
                }

                for group_name in &self.parameters.groups {
                    // if the user exists in "active_users"
                    if groups_of_user.contains(group_name) {
                        continue;
                    }

                    if rng.gen::<f64>() < self.parameters.join_chance {
                        let group_exists = match user.group_exists(group_name.clone()) {
                            Ok(a) => a,
                            Err(_) => continue,
                        };
                        if !group_exists {
                            log::info!("{} -> Creating group {}", self.username, group_name);

                            match user.create_group(group_name.clone()) {
                                Ok(_) => {}
                                Err(e) => { log::error!("Error creating group: {}", e); }
                            }

                            continue;
                        }

                        if self.parameters.external_join {
                            match user.get_group_info(group_name.clone()) {
                                Ok(group_info) => {
                                    log::info!("{} IS JOINING GROUP {}", self.username, group_name);
                                    //log::info!("\tGroup Info downloaded");

                                    match user.external_join(group_name.to_string(), group_info) {
                                        Ok(_) => {
                                            //log::info!("{} joined group", self.username);
                                            //ClientAgent::write_timestamp(self.username.clone(), ec, group_name.clone(), CGKAAction::Join);
                                        },
                                        Err(error) => {
                                            log::error!("Error performing external join: {}", error);
                                        }
                                    };
                                    //current_members.push(self.username.clone());
                                },
                                Err(error) => {
                                    if error.contains("403") {
                                        log::info!("{} -> Group {} is in use", self.username, group_name);
                                    }
                                }
                            }
                        }
                    }
                }

                drop(user);
            }
            //drop(current_members);
        }
    }

    pub fn write_timestamp(username: String, action_record: ActionRecord) {
        let ActionRecord {group_name, action, epoch_change, elapsed_time} = action_record;

        let mut to_write = group_name.clone();
        to_write.push_str(" ");
        to_write.push_str(epoch_change.epoch.to_string().as_str());
        to_write.push_str(" ");
        to_write.push_str(&username);
        to_write.push_str(" ");
        to_write.push_str(format!("{}", action).as_str());
        to_write.push_str(" ");
        to_write.push_str(epoch_change.timestamp.to_string().as_str());
        to_write.push_str(" ");
        to_write.push_str(elapsed_time.to_string().as_str());

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(format!("logs/{}.txt", username.clone()))
            .unwrap();

        if let Err(e) = writeln!(file, "{}", to_write) {
            eprintln!("Couldn't write to file: {}", e);
        }
    }

    pub fn scale(&self,  value: f64, user: &User, group_name: String) -> f64 {
        if self.parameters.scale {
            let members = user.members_of_group(group_name.clone()).len() + 1;
            value / (members+1) as f64
        }

        else  {
            value
        }
    }
}