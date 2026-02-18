use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use crate::config::{OrchestratedParameters};
use std::cmp::min;

#[derive(Debug, Clone, PartialEq)]
pub enum NextAction {
    Inactive,
    Listen,
    Commit
}

#[derive(Debug, Clone)]
pub struct Orchestrator {
    pub name: String,
    pub pending_acks: usize,
    pub parameters: OrchestratedParameters,
    pub known_members: Vec<String>, 
}

impl Orchestrator {
    pub fn new(name: String, parameters: OrchestratedParameters, known_members: Vec<String>) -> Self {
        let mut known_members = known_members;
        known_members.resize(parameters.max_active_processers, "".to_string());
        Orchestrator {
            name,
            pending_acks: 0,
            parameters,
            known_members,
        }
    }

    pub fn process_commit(&mut self, seed: &[u8], total_members: usize, new_members: Vec<String>) -> NextAction {

        let mut seed_array = [0u8; 32];
        seed_array[..seed.len().min(32)].copy_from_slice(&seed[..seed.len().min(32)]);

        //tracing::info!("Processing commit with total_members={}, new_members={:?}, seed={:?}", total_members, new_members, seed_array);

        let mut rng = StdRng::from_seed(seed_array);

        for (_i, name) in new_members.iter().enumerate() {

            let current_amount_of_active = self.known_members.iter().filter(|x| *x != "").count();

            // Reservoir sampling
            let sample = match self.parameters.auth_policy {
                crate::config::AuthorizationPolicy::First =>  {
                    // First: new members never become committers
                    rng.gen_range(min(self.parameters.max_active_committers, total_members-1)..total_members)
                },
                crate::config::AuthorizationPolicy::Last => {
                    // Last always becomes committer
                    0
                }
                crate::config::AuthorizationPolicy::Random => {
                    rng.gen_range(0..total_members)
                }
            };

            let new_position = current_amount_of_active;

            // If committers are not yet full, new member becomes committer and processor
            if current_amount_of_active < self.parameters.max_active_committers {
                self.known_members[new_position] = name.clone();
            }

            // If both committers and processors are full, chance to become any of them
            else if sample < self.parameters.max_active_processers {
                let previous_member = self.known_members[sample].clone();

                // If there was a previous member at that position and there is space left, move it to the new member's position
                if previous_member != "" && current_amount_of_active < self.parameters.max_active_processers {
                    self.known_members[new_position] = previous_member;
                }
                self.known_members[min(new_position, sample)] = name.clone();

            }
        }

        let active_position = self.known_members.iter().position(|x| x == &self.name);

        if active_position.is_none() {
            tracing::debug!("Action: {:?}. State: active_position={:?}", NextAction::Inactive, active_position);
            return NextAction::Inactive;
        }

        let active_position = active_position.unwrap();

        let mut who_acts = rng.gen_range(0..min(self.parameters.max_active_committers, total_members));

        let mut rerolled = false;
        // If there were multiple new members, some of them may not receive ACKs due to synchronization issues
        // Preventively, do not allow next committer to be a new member if there were multiple new members
        while new_members.len() > 1 && new_members.contains(&self.known_members[who_acts]) {
            // Try again
            who_acts = rng.gen_range(0..min(self.parameters.max_active_committers, total_members));
            rerolled = true;
        }

        let result = if active_position == who_acts {
            // Expect as many acks as "falses" in new_members -- to omit myself
            self.pending_acks = new_members.iter().filter(|x| x.to_string() != self.name).count();

            if self.pending_acks == 0 {
                NextAction::Commit
            }
            else {
                NextAction::Listen
            }
        }
        else {
            NextAction::Listen
        };

        tracing::info!(
            "Action: {:?}. Winner={} (EXPECTED: {}), Me={}/{}{} - Rerolled={}",
            result,
            who_acts,
            self.known_members.get(who_acts).cloned().unwrap_or("UNKNOWN".to_string()),
            active_position,
            min(self.parameters.max_active_committers, total_members),
            if self.pending_acks > 0 { ", (PENDING)" } else { "" },
            rerolled
        );

                // If there is a "" followed by a non-"" member, error in the state
        for i in 0..(self.known_members.len()-1) {
            if self.known_members[i] == "" && self.known_members[i+1] != "" {
                tracing::error!("Inconsistent state after commit: {:?}", self.known_members);
            }
        }

            
        if self.known_members.get(who_acts).cloned().unwrap_or("UNKNOWN".to_string()) == "" {
            tracing::info!("WINNER NOT FOUND: {:?}", self.known_members);
        }

        result
    }

    pub fn remove_members(&mut self, members_to_remove: Vec<String>) {
        // Remove members from known_members
        // if any member is before the active_position, decrement active_position

        let active_position = self.known_members.iter().position(|x| x == &self.name);
        if active_position.is_none() {
            //tracing::info!("I am not active -- no removals needed");
            return;
        }

        self.known_members.retain(|member| !members_to_remove.contains(member));
        self.known_members.resize(self.parameters.max_active_processers, "".to_string());

        // If there is a "" followed by a non-"" member, error in the state
        for i in 0..(self.known_members.len()-1) {
            if self.known_members[i] == "" && self.known_members[i+1] != "" {
                tracing::error!("Inconsistent state after removals: {:?}", self.known_members);
            }
        }
    }

    pub fn process_ack(&mut self) -> NextAction {
        if self.pending_acks == 0 {
            // Not waiting for acks -- keep listening
            return NextAction::Listen;
        }
        else if self.pending_acks > 0 {
            self.pending_acks -= 1;
        }

        tracing::debug!(
            "After ACK: pending_acks={}",
            self.pending_acks
        );

        if self.pending_acks == 0 {
            // All acks received -- can commit
            NextAction::Commit
        }
        else {
            // Still waiting for acks -- keep listening
            NextAction::Listen
        }


    }
}