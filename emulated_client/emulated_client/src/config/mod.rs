pub mod user_parameters;

use config::Config;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub enum DSType {
    Request,
    PubSubMQTT(String),
    GossipSub(GossipSubConfig),
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub enum Directory {
    Server,
    Kademlia
}

#[derive(Clone, Debug, PartialEq)]
pub enum Paradigm {
    Commit,
    Propose
}
#[derive(Debug, Clone, PartialEq)]
pub enum AuthorizationPolicy {
    Random,
    First,
    Last
}

#[derive(Debug, Clone, PartialEq)]
pub enum Behaviour {
    Independent(IndependentParameters),
    Orchestrated(OrchestratedParameters),
}

#[derive(Debug, Clone, PartialEq)]

pub struct IndependentParameters {
    pub(crate) sleep_millis_min: i64,
    pub(crate) sleep_millis_max: i64,
    pub(crate) issue_update_chance: f64,

    pub(crate) scale: bool,
    pub(crate) auth_policy: AuthorizationPolicy,
}

#[derive(Debug, Clone, PartialEq)]

pub struct OrchestratedParameters {
    pub(crate) max_active_committers: usize,
    pub(crate) max_active_processers: usize,

    pub(crate) auth_policy: AuthorizationPolicy,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, Debug)]
pub struct GossipSubConfig {
    pub(crate) address: String,
    pub directory: Directory,
    pub(crate) rendezvous_address: String,
    pub(crate) rendezvous_id: String,
}

impl From<String> for AuthorizationPolicy {
    fn from(policy: String) -> Self {
        match policy.as_str() {
            "first" => AuthorizationPolicy::First,
            "last" => AuthorizationPolicy::Last,
            _ => AuthorizationPolicy::Random,
        }
    }
}

impl From<String> for Paradigm {
    fn from(paradigm: String) -> Self {
        match paradigm.as_str() {
            "propose" => Paradigm::Propose,
            _ => Paradigm::Commit,
        }
    }
}

impl From<String> for Directory {
    fn from(dir: String) -> Self {
        match dir.as_str() {
            "kademlia" => Directory::Kademlia,
            _ => Directory::Server,
        }
    }
}

impl IndependentParameters {
    pub fn new_from_settings(settings: &Config) -> Result<Self, String> {
        let default = IndependentParameters::default();

        let sleep_millis_min = settings.get_int("independent.sleep_millis_min").unwrap_or(default.sleep_millis_min);
        let sleep_millis_max = settings.get_int("independent.sleep_millis_max").unwrap_or(default.sleep_millis_max);
        let issue_update_chance = settings.get_float("independent.issue_update_chance").unwrap_or(default.issue_update_chance);

        let scale = settings.get_bool("independent.scale").unwrap_or(default.scale);
        let auth_policy = settings.get_string("independent.auth_policy")
            .map(|policy| AuthorizationPolicy::from(policy))
            .unwrap_or(default.auth_policy.clone());

        Ok(Self {
            sleep_millis_min,
            sleep_millis_max,
            issue_update_chance,
            scale,
            auth_policy,
        })
    }
}

impl OrchestratedParameters {
    pub fn new_from_settings(settings: &Config) -> Result<Self, String> {
        let default = OrchestratedParameters::default();

        let max_active_committers = settings.get_int("orchestrator.max_active_committers")
            .unwrap_or(default.max_active_committers as i64) as usize;
        let max_active_processers = settings.get_int("orchestrator.max_active_processers")
            .unwrap_or(default.max_active_processers as i64) as usize;
                let auth_policy = settings.get_string("orchestrator.auth_policy")
            .map(|policy| AuthorizationPolicy::from(policy))
            .unwrap_or(default.auth_policy.clone());

        Ok(Self {
            max_active_committers,
            max_active_processers,
            auth_policy,
        })
    }
}

impl GossipSubConfig {
    pub fn new_from_settings(settings: &Config) -> Result<Self, String> {
        let default = GossipSubConfig::default();


        let address = settings.get_string("gossipsub.address").unwrap_or(default.address);
        let directory = settings.get_string("gossipsub.directory")
            .map(|dir| Directory::from(dir)).unwrap_or(default.directory);

        let rendezvous_address = settings.get_string("gossipsub.rendezvous_address").unwrap_or(default.rendezvous_address);
        let rendezvous_id = settings.get_string("gossipsub.rendezvous_id").unwrap_or(default.rendezvous_id);

        Ok(Self {
            address,
            directory,
            rendezvous_address,
            rendezvous_id
        })
    }
}

impl Default for IndependentParameters {
    fn default() -> Self {
        Self {
            sleep_millis_min: 10000,
            sleep_millis_max: 30000,
            scale: false,
            issue_update_chance: 0.5,
            auth_policy: AuthorizationPolicy::Random,
        }
    }
}

impl Default for OrchestratedParameters {
    fn default() -> Self {
        Self {
            max_active_committers: 100,
            max_active_processers: 100,
            auth_policy: AuthorizationPolicy::Random,
        }
    }
}

impl Default for GossipSubConfig {
    fn default() -> Self {
        Self {
            address: "0.0.0.0".to_string(),
            rendezvous_address: "/dns/rendezvous/tcp/62649".to_string(),
            rendezvous_id: "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN".to_string(),
            directory: Directory::Kademlia,
        }
    }
}