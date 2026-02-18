
use openmls_traits::OpenMlsProvider;
use tokio::sync::mpsc::{Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use rand::Rng;
use super::{ClientAgent, CGKAAction};
use crate::config::{DSType, Paradigm};
use crate::config::user_parameters::UserParameters;
use crate::user::{User};

pub struct OrchestratedClientAgent<P: OpenMlsProvider> {
    pub username: String,
    user: Arc<Mutex<User<P>>>,
    parameters: UserParameters,
    receiver: Receiver<String>
}

impl<P: OpenMlsProvider> OrchestratedClientAgent<P> {
    pub fn new(
        name: String,
        user: Arc<Mutex<User<P>>>,
        parameters: UserParameters,
        receiver: Receiver<String>
    ) -> Self {
        OrchestratedClientAgent {
            username: name,
            user,
            parameters,
            receiver
        }
    }
}
 

impl<P: OpenMlsProvider> ClientAgent<P> for OrchestratedClientAgent<P> {
    fn run(&mut self) {
        let mut rng = rand::thread_rng();

        //let mut remaining_groups = self.parameters.groups.clone();
        //tracing::info!("Starting orchestrated client agent for {}", self.username);


        {
            let mut user = self.user.lock().unwrap();
            user.subscribe_welcome().unwrap();
            match user.register() {
                Ok(_) => {}
                Err(e) => {
                    if e.contains("409") {
                        tracing::info!("User is in use");
                        panic!("User is in use");
                    }
                    panic!("Error registering user: {}", e);
                }
            }
            user.create_kp().unwrap();

            // Wait for other users to start
            thread::sleep(Duration::from_secs(10));

            // At the start, attempt to create group
            for group_name in &self.parameters.groups {
                let group_exists = match user.get_group_info(group_name.clone()) {
                    Ok(_) => {
                        true
                    },
                    Err(e) => {
                        if e.contains("403") {
                            true
                        } else {
                            false
                        }
                    },
                };
                if !group_exists {
                    tracing::info!("Creating group {}", group_name);

                    match user.create_group(group_name.clone()) {
                        Ok(_) => {}
                        Err(e) => { tracing::error!("Error creating group: {}", e); }
                    }

                    continue;
                }
            }
            drop(user);
        }


        loop {
            let group_name = self.receiver.blocking_recv().unwrap();
            let mut user = self.user.lock().unwrap();

            if self.parameters.delivery_service == DSType::Request {
                //tracing::info!("Requesting updates for {}", self.username);
                user.fetch_updates().expect("Error updating group");
            }

            // Issue an update

            // Handle max members
            if self.parameters.max_members != 0 {
                // If max members is reached, ignore probabilities and add/remove/update with equal probability
                if user.number_of_members(group_name.clone()) >= self.parameters.max_members {
                    //tracing::info!("Max members reached in {}, skipping update", group_name);
                    match self.remove_from_group(&mut user, group_name.clone()) {
                        Ok(_) => {}
                        Err(e) => { tracing::error!("Error removing user: {}", e); }
                    }

                    continue;
                }
            }
            loop {
                let random_value: f64 = rng.gen_range(0.0..1.0);

                let result = if random_value < self.parameters.invite_chance {
                    //invite random user from group
                    self.invite_user(&mut user, group_name.clone())
                } else if random_value < (self.parameters.invite_chance + self.parameters.remove_chance) {
                    //remove random user from group
                    self.remove_from_group(&mut user, group_name.clone())
                } else {
                    //update group
                    self.update_group(&mut user, group_name.clone())
                };

                match result {
                    Ok(_) => {
                        if self.parameters.paradigm == Paradigm::Commit {
                            // Commit has been performed
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!("Error performing action: {}", e);
                        thread::sleep(Duration::from_secs(2));
                        // Try again
                    }
                }

                if self.parameters.paradigm == Paradigm::Propose {
                    // Commit to proposals
                    if user.pending_proposals(group_name.clone()).unwrap() < self.parameters.proposals_per_commit { continue; }

                    tracing::info!("Committing to Proposals in {}", group_name);
                    match user.commit_to_proposals(group_name.clone(), self.parameters.proposals_per_commit) {
                        Ok(_) => {
                            // Commit has been performed
                            break;
                        },
                        Err(error) => {
                            tracing::error!("Error committing to proposals: {}", error);
                        },
                    }
                }
            }

            /*if rng.gen::<f64>() < self.scale(self.parameters.message_chance, &user, group_name.clone()) {
                // Send Application message
                tracing::info!("Sending message in {}", group_name);
                let length = rng.gen_range(
                    self.parameters.message_length_min..self.parameters.message_length_max
                );

                let message = (&mut rng).sample_iter(&Alphanumeric)
                    .take(length)
                    .map(char::from)
                    .collect();
                match user.send_application_msg(message, group_name.clone()) {
                    Ok(()) => {}
                    Err(e) => {tracing::error!("Error sending message: {}", e);}
                }
            }*/

            drop(user);
        }
    }

    fn invite_user(&self, user: &mut User<P>, group_name: String) -> Result<(), String> {
        let mut rng = rand::thread_rng();
        match user.update_clients() {
            Ok(_) => {}
            Err(e) => { return Err(format!("Error updating clients: {}", e)); }
        };

        //add random user from group
        let candidates = user.not_members_of_group(group_name.clone());

        //tracing::info!("{}: {} candidates to invite", candidates.len());

        if candidates.len() == 0 {
            return Err("No candidates to invite".to_string());
        }

        let user_to_add = candidates[rng.gen_range(0..candidates.len())].clone();
        match self.parameters.paradigm {
            Paradigm::Commit => {
                tracing::info!("Inviting {} to {}", user_to_add, group_name);
                match user.invite(user_to_add.clone(), group_name.clone()) {
                    Ok(_) => {Ok(())},
                    Err(error) => {
                        return Err(format!("Error inviting user: {}", error));
                    },
                }
            }
            Paradigm::Propose => {
                tracing::info!("Proposing Invite {} to {}", user_to_add, group_name);
                let action = CGKAAction::Invite(user_to_add, 0);
                match user.propose(action, group_name.clone()) {
                    Ok(_) => {Ok(())},
                    Err(error) => {
                        return Err(format!("Error proposing Invite: {}", error));
                    },
                }
            }
        }
    }

    fn remove_from_group(&self, user: &mut User<P>, group_name: String) -> Result<(), String>{
        let mut rng = rand::thread_rng();

        let members = user.members_of_group(group_name.clone());
        if members.len() == 0 {
            return Err("No candidates to remove".to_string());
        }

        let user_to_remove = members[rng.gen_range(0..members.len())].clone();

        match self.parameters.paradigm {
            Paradigm::Commit => {
                tracing::info!("Removing {} from {}", user_to_remove, group_name);
                match user.remove(user_to_remove.clone(), group_name.clone()) {
                    Ok(_) => { Ok(()) },
                    Err(error) => {
                        Err(format!("Error removing user: {}", error))
                    },
                }
            }
            Paradigm::Propose => {
                tracing::info!("Proposing Remove {} from {}", user_to_remove, group_name);
                let action = CGKAAction::Remove(user_to_remove, 0);
                match user.propose(action, group_name.clone()) {
                    Ok(_) => { Ok(()) },
                    Err(error) => {
                        Err(format!("Error proposing Remove: {}", error))
                    },
                }
            }
        }
    }

    fn update_group(&self, user: &mut User<P>, group_name: String) -> Result<(), String> {
        match self.parameters.paradigm {
            Paradigm::Commit => {
                tracing::info!("Updating in {}", group_name);
                match user.update_state(group_name.clone()) {
                    Ok(_) => {Ok(())},
                    Err(error) => {
                        Err(format!("Error updating user: {}", error))
                    },
                }
            }
            Paradigm::Propose => {

                let action = CGKAAction::Update(0);
                tracing::info!("Proposing Update in {}", group_name);
                match user.propose(action, group_name.clone()) {
                    Ok(_) => {Ok(())},
                    Err(error) => {
                        Err(format!("Error proposing user: {}", error))
                    },
                }
            }
        }
    }

    fn username(&self) -> String {
        self.username.clone()
    }
}