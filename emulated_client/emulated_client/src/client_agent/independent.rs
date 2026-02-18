use std::sync::{Arc, Mutex};
use std::{thread};

use std::time::Duration;
use openmls_traits::OpenMlsProvider;
use rand::Rng;
use super::{ClientAgent, CGKAAction};

use rand::distributions::Alphanumeric;
use crate::config::{Behaviour, DSType, Paradigm};
use crate::config::user_parameters::UserParameters;
use crate::user::{User};

pub struct IndependentClientAgent<P: OpenMlsProvider> {
    username: String,
    user: Arc<Mutex<User<P>>>,
    parameters: UserParameters,
    pub existing_groups: Vec<String>,
}

impl<P: OpenMlsProvider> IndependentClientAgent<P> {
    pub fn new(
        name: String,
        user: Arc<Mutex<User<P>>>,
        parameters: UserParameters,
    ) -> Self {

        IndependentClientAgent {
            username: name,
            user,
            parameters,
            existing_groups: Vec::new(),
        }
    }

    pub fn scale(&self, value: f64, members: usize) -> f64 {
        let independent_parameters = match self.parameters.behaviour {
            Behaviour::Independent(ref params) => params.clone(),
            _ => unreachable!(),
        };

        if independent_parameters.scale {
            value / (members+1) as f64
        }

        else  {
            value
        }
    }
}

impl<P: OpenMlsProvider> ClientAgent<P> for IndependentClientAgent<P> {
    fn run(&mut self) {
        tracing::debug!("Starting user {}", self.username);

        let mut rng = rand::thread_rng();

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
            drop(user);
        }

        let independent_parameters = match self.parameters.behaviour {
            Behaviour::Independent(ref params) => params.clone(),
            _ => unreachable!(),
        };

        loop {

            let millis_to_sleep = rng.gen_range(independent_parameters.sleep_millis_min..independent_parameters.sleep_millis_max) as u64;
            thread::sleep(Duration::from_millis(millis_to_sleep));
            {
                let mut user = self.user.lock().unwrap();
                let groups_of_user = user.list_of_groups();


                if groups_of_user.len() == self.parameters.groups.len()
                    && independent_parameters.issue_update_chance == 0.0 {
                    tracing::info!("Participation completed, exiting early");
                    return;
                }

                if self.parameters.delivery_service == DSType::Request {
                    tracing::debug!("Requesting updates for {}", self.username);
                    user.fetch_updates().expect("Error updating group");
                }

                for group_name in user.group_list() {

                    match user.is_authorised(group_name.clone(), independent_parameters.auth_policy.clone()) {
                        Ok(is_auth) => {
                            if !is_auth {
                                continue;
                            }
                        }

                        Err(e) => {tracing::error!("Error checking authorisation: {:?}", e); continue;}
                    }

                    let members = user.number_of_members(group_name.clone());

                    if rng.gen::<f64>() < self.scale(independent_parameters.issue_update_chance, members) {
                        // Issue an update

                        // Handle max members
                        if self.parameters.max_members != 0 {
                            // If max members is reached, ignore probabilities and add/remove/update with equal probability
                            if user.number_of_members(group_name.clone()) >= self.parameters.max_members {

                                match self.remove_from_group(&mut user, group_name.clone()) {
                                    Ok(_) => {}
                                    Err(e) => { tracing::error!("Error removing user: {}", e); }
                                }

                                continue;
                            }
                        }


                        let random_value: f64 = rng.gen_range(0.0..1.0);

                        if random_value < self.parameters.invite_chance {
                            //invite random user from group
                            match self.invite_user(&mut user, group_name.clone()) {
                                Ok(_) => {}
                                Err(e) => {tracing::error!("Error inviting user: {}", e);}
                            }
                        }
                        else if random_value < (self.parameters.invite_chance + self.parameters.remove_chance) {
                            //remove random user from group
                            match self.remove_from_group(&mut user, group_name.clone()) {
                                Ok(_) => {}
                                Err(e) => {tracing::error!("Error removing user: {}", e);}
                            }
                        }
                        else {
                            //update group
                            match self.update_group(&mut user, group_name.clone()) {
                                Ok(_) => {}
                                Err(e) => {
                                    tracing::error!("Error updating group: {}", e);

                            }
                            }
                        }

                        if self.parameters.paradigm != Paradigm::Propose {continue;}
                    }

                    if rng.gen::<f64>() < self.scale(independent_parameters.issue_update_chance, members)
                        && self.parameters.paradigm == Paradigm::Propose {
                        // Commit to proposals
                        if user.pending_proposals(group_name.clone()).unwrap() <= self.parameters.proposals_per_commit {continue;}

                        if let Err(_) = user.get_group_info(group_name.clone()) {continue;}
                        tracing::info!("Committing to Proposals in {}", group_name);
                        match user.commit_to_proposals(group_name.clone(), self.parameters.proposals_per_commit) {
                            Ok(_) => {},
                            Err(error) => {
                                tracing::error!("Error committing to proposals: {}", error);
                                user.publish_group_info(group_name.clone()).unwrap();
                            },
                        }
                        continue;
                    }

                    if rng.gen::<f64>() < self.scale(self.parameters.message_chance, members) {
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
                    }
                }

                for group_name in &self.parameters.groups {
                    // if the user exists in "active_users"
                    if groups_of_user.contains(group_name) {
                        continue;
                    }

                    if rng.gen::<f64>() < self.parameters.join_chance {
                    
                        let group_exists = {
                            if self.existing_groups.contains(group_name) {
                                true
                            }
                            else {
                                match user.group_exists(group_name.clone()) {
                                    Ok(a) => a,
                                    Err(_) => continue,
                                }
                            }
                        };
                        if !group_exists {
                            tracing::info!("Creating group {}", group_name);

                            match user.create_group(group_name.clone()) {
                                Ok(_) => {}
                                Err(e) => { tracing::error!("Error creating group: {}", e); }
                            }

                            continue;
                        }

                        self.existing_groups.push(group_name.clone());
                        if self.parameters.external_join {
                            match user.get_group_info(group_name.clone()) {
                                Ok(group_info) => {
                                    match user.external_join(group_name.to_string(), group_info) {
                                        Ok(_) => {},
                                        Err(error) => {
                                            tracing::error!("Error performing external join: {}", error);
                                        }
                                    };
                                },
                                Err(error) => {
                                    if error.contains("403") {
                                        tracing::info!("Group {} is in use", group_name);
                                    }
                                }
                            }
                        }
                    }
                }

                drop(user);
            }
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

        tracing::debug!("{} candidates to invite", candidates.len());

        if candidates.len() == 0 {
            return Err("No candidates to invite".to_string());
        }

        let user_to_add = candidates[rng.gen_range(0..candidates.len())].clone();
        match self.parameters.paradigm {
            Paradigm::Commit => {
                //Mutex
                if let Err(_) = user.get_group_info(group_name.clone()) {

                    return Ok(());
                }

                tracing::info!("Inviting {} to {}", user_to_add, group_name);
                match user.invite(user_to_add.clone(), group_name.clone()) {
                    Ok(_) => {Ok(())},
                    Err(error) => {
                        user.publish_group_info(group_name.clone()).unwrap();
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
                //Mutex
                if let Err(_) = user.get_group_info(group_name.clone()) {
                    return Ok(());
                }

                tracing::info!("Removing {} from {}", user_to_remove, group_name);
                match user.remove(user_to_remove.clone(), group_name.clone()) {
                    Ok(_) => { Ok(()) },
                    Err(error) => {
                        user.publish_group_info(group_name.clone()).unwrap();
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
                //Mutex
                if let Err(_) = user.get_group_info(group_name.clone()) {
                    return Ok(());
                }

                tracing::info!("Updating in {}", group_name);
                match user.update_state(group_name.clone()) {
                    Ok(_) => {Ok(())},
                    Err(error) => {
                        user.publish_group_info(group_name.clone()).unwrap();
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