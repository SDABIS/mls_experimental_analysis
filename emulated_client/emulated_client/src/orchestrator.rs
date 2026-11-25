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

        let eligible_committers: Vec<usize> = self
            .known_members
            .iter()
            .take(min(self.parameters.max_active_committers, total_members))
            .enumerate()
            .filter(|(_, member)| {
                !member.is_empty() && (new_members.len() <= 1 || !new_members.contains(member))
            })
            .map(|(position, _)| position)
            .collect();

        if eligible_committers.is_empty() {
            tracing::error!("No eligible committer: {:?}", self.known_members);
            return NextAction::Listen;
        }

        let who_acts = eligible_committers[rng.gen_range(0..eligible_committers.len())];

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

        tracing::debug!(
            "Action: {:?}. Winner={} (EXPECTED: {}), Me={}/{}{}",
            result,
            who_acts,
            self.known_members.get(who_acts).cloned().unwrap_or("UNKNOWN".to_string()),
            active_position,
            min(self.parameters.max_active_committers, total_members),
            if self.pending_acks > 0 { ", (PENDING)" } else { "" },
        );

                // If there is a "" followed by a non-"" member, error in the state
        for i in 0..(self.known_members.len()-1) {
            if self.known_members[i] == "" && self.known_members[i+1] != "" {
                tracing::error!("Inconsistent state after commit: {:?}", self.known_members);
            }
        }

            
        result
    }

    pub fn remove_members(&mut self, members_to_remove: Vec<String>) {
        // Remove members from known_members
        // if any member is before the active_position, decrement active_position

        let active_position = self.known_members.iter().position(|x| x == &self.name);
        if active_position.is_none() {
            tracing::debug!("I am not active -- no removals needed");
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

#[cfg(test)]
mod tests {
    use super::{NextAction, Orchestrator};
    use crate::config::{AuthorizationPolicy, OrchestratedParameters};

    fn roster(count: usize) -> Vec<String> {
        (0..count).map(|i| format!("member-{i}")).collect()
    }

    fn settings(committers: usize, policy: AuthorizationPolicy) -> OrchestratedParameters {
        OrchestratedParameters {
            max_active_committers: committers,
            max_active_processers: 20,
            auth_policy: policy,
        }
    }

    #[test]
    fn skips_an_empty_tenth_slot() {
        let members = roster(9);
        let params = settings(10, AuthorizationPolicy::Random);
        // This seed selected the empty tenth slot in the original scheduler.
        let seed = [5; 32];
        let winners = members.iter().filter(|name| {
            let mut node = Orchestrator::new((*name).clone(), params.clone(), members.clone());
            node.process_commit(&seed, 98, vec![]) == NextAction::Commit
        });
        assert_eq!(winners.count(), 1);
    }

    #[test]
    fn a_single_joiner_can_commit() {
        let members = roster(10);
        let params = settings(10, AuthorizationPolicy::Last);
        let joiner = String::from("joiner");

        // A lone joiner should be able to win at least one of these draws.
        assert!((0..=u8::MAX).any(|byte| {
            let mut node = Orchestrator::new(joiner.clone(), params.clone(), members.clone());
            node.process_commit(&[byte; 32], 11, vec![joiner.clone()]) == NextAction::Commit
        }));
    }

    #[test]
    fn one_existing_member_waits_for_both_joiners() {
        let members = roster(10);
        let params = settings(10, AuthorizationPolicy::Last);
        let joiners = vec![String::from("joiner-a"), String::from("joiner-b")];

        for byte in 0..=u8::MAX {
            let mut waiters = 0;
            // Everyone sees the same draw, including both new members.
            for name in members.iter().chain(&joiners) {
                let mut node = Orchestrator::new(name.clone(), params.clone(), members.clone());
                let action = node.process_commit(&[byte; 32], 12, joiners.clone());
                assert_ne!(action, NextAction::Commit);
                if node.pending_acks > 0 {
                    // Only an existing member may win, and it must wait for both ACKs.
                    assert!(members.contains(name));
                    assert_eq!(node.pending_acks, 2);
                    assert_eq!(node.process_ack(), NextAction::Listen);
                    assert_eq!(node.process_ack(), NextAction::Commit);
                    waiters += 1;
                }
            }
            assert_eq!(waiters, 1);
        }
    }

    #[test]
    fn listens_when_no_committer_is_eligible() {
        // Only slot zero can commit. The processor occupies slot one.
        let members = vec![String::new(), String::from("processor")];
        let mut node = Orchestrator::new(
            String::from("processor"),
            settings(1, AuthorizationPolicy::Random),
            members,
        );
        assert_eq!(node.process_commit(&[0; 32], 2, vec![]), NextAction::Listen);
    }
}
