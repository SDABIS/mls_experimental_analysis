pub mod independent;
pub mod orchestrated;

use crate::user::{EpochChange, User};
use std::fmt::{Debug, Display, Formatter};
use openmls::prelude::{CryptoTime, ProcessCryptoTime, MergeTime};
use openmls_traits::OpenMlsProvider;

#[derive(Debug, Clone)]
pub struct ActionRecord {
    pub group_name: String,
    pub action: CGKAAction,
    pub epoch_change: EpochChange,
    pub elapsed_time: u128,
    pub num_users: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CGKAAction {
    // Proposed action
    Propose(Box<CGKAAction>),
    // Proposals committed, size of commit
    Commit(Vec<CGKAAction>, usize),
    // Size of commit, size of group info
    Join(usize, usize),
    // Size of commit
    Update(usize),
    // Invited user, size of commit
    Invite(String, usize),
    // Removed user, size of commit
    Remove(String, usize),
    // User who committed
    Process(String),
    // User who proposed
    StoreProp(String),
    // User who committed
    Welcome(String, usize),
    // message size, time taken (crypto), time taken (export), time taken (merge)
    DeepCommit(usize, CryptoTime, u128, MergeTime),
    // time taken (crypto), time taken (merge)
    DeepProcess(String, ProcessCryptoTime, MergeTime),
    Create,

    // Attempted commit
    CommitAttempt(Box<CGKAAction>),
}

impl Display for CGKAAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CGKAAction::Propose(action) => {write!(f, "Propose {}", action)},
            CGKAAction::Commit(proposals, size) => {
                let number_of_proposals = proposals.len();
                write!(f, "Commit {number_of_proposals} {size}")
            }
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
            CGKAAction::CommitAttempt(action) => {write!(f, "CommitAttempt {}", action)},
            CGKAAction::DeepCommit(size, crypto, export, merge) => {
                let CryptoTime {
                    total,
                    validation,
                    apply_proposals, 
                    tree,
                    encrypt_path, 
                    sign,
                    schedule,
                    welcome,
                    storage,
                    encrypt
                } = crypto;
                let MergeTime {
                    total: total_merge,
                    merge,
                    storage: storage_merge
                } = merge;

                write!(f, "DeepCommit {size} || {total} {validation} {apply_proposals} {tree} {encrypt_path} {sign} {schedule} {welcome} || {storage} {encrypt} || {export} || {total_merge} {merge} {storage_merge}" )
            },
            CGKAAction::DeepProcess(user, crypto, merge) => {
                let ProcessCryptoTime {
                    total,
                    decrypt,
                    verify,
                    validation,
                    apply_proposals, 
                    tree,
                    schedule,
                    decrypt_path
                } = crypto;

                let MergeTime {
                    total: total_merge,
                    merge,
                    storage: storage_merge
                } = merge;

                write!(f, "DeepProcess {user} || {total} {decrypt} {verify} {validation} {apply_proposals} {tree} {decrypt_path} {schedule} || {total_merge} {merge} {storage_merge}" )
            }
        }
    }
}

pub trait ClientAgent<P: OpenMlsProvider> {
    fn run(&mut self);
    fn update_group(&self, user: &mut User<P>, group_name: String) -> Result<(), String>;
    fn invite_user(&self, user: &mut User<P>, group_name: String) -> Result<(), String>;
    fn remove_from_group(&self, user: &mut User<P>, group_name: String) -> Result<(), String>;

    fn username(&self) -> String;
}