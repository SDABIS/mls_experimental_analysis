
use config::Config;
use url::Url;
use crate::config::{Behaviour, DSType, GossipSubConfig, IndependentParameters, OrchestratedParameters, Paradigm};

#[derive(Debug, Clone, PartialEq)]
pub struct UserParameters {
    pub delivery_service: DSType,
    pub server_url: Url,

    pub(crate) external_join: bool,
    pub(crate) join_chance: f64,
    pub(crate) message_chance: f64,
    pub(crate) message_length_min: usize,
    pub(crate) message_length_max: usize,

    pub(crate) groups: Vec<String>,
    pub(crate) max_members: usize,

    pub(crate) invite_chance: f64,
    pub(crate) remove_chance: f64,
    pub(crate) update_chance: f64,

    pub behaviour: Behaviour,

    pub(crate) paradigm: Paradigm,
    pub(crate) proposals_per_commit: usize,

    pub(crate) mqtt_url: String,

}

impl UserParameters {
    pub fn new_from_settings(settings: &Config) -> Result<Self, String> {
        let default = UserParameters::default();

        let mqtt_url = settings.get_string("mqtt.url").unwrap_or(default.mqtt_url);
        let delivery_service = settings.get_string("cgka.ds").map(|ds| match ds.as_str() {
            "mqtt" => {
                DSType::PubSubMQTT(mqtt_url.clone())
            },
            "gossipsub" => {
                let gossipsub_config = GossipSubConfig::new_from_settings(settings).unwrap();
                DSType::GossipSub(gossipsub_config)
            },
            _ => DSType::Request,
        }).unwrap_or(default.delivery_service);

        let server_url = settings.get_string("http_server.url").map(|url| Url::parse(&url).unwrap())
            .unwrap_or(default.server_url);

        let external_join = settings.get_bool("cgka.external_join").unwrap_or(default.external_join);
        let join_chance = settings.get_float("cgka.join_chance").unwrap_or(default.join_chance);
        let message_chance = settings.get_float("cgka.message_chance").unwrap_or(default.message_chance);
        let message_length_min = settings.get_int("cgka.message_length_min").unwrap_or(default.message_length_min as i64) as usize;
        let message_length_max = settings.get_int("cgka.message_length_max").unwrap_or(default.message_length_max as i64) as usize;

        let behaviour = settings.get_string("meta.behaviour")
            .map(|b| match b.as_str() {
                "orchestrated" => {
                    let orchestrated_parameters = OrchestratedParameters::new_from_settings(settings).unwrap();
                    Behaviour::Orchestrated(orchestrated_parameters)
                },
                _ => {
                    let independent_parameters = IndependentParameters::new_from_settings(settings).unwrap();
                    Behaviour::Independent(independent_parameters)
                }
            })
            .unwrap_or(default.behaviour);

        /*let sleep_millis_min = settings.get_int("cgka.sleep_millis_min").unwrap_or(default.sleep_millis_min);
        let sleep_millis_max = settings.get_int("cgka.sleep_millis_max").unwrap_or(default.sleep_millis_max);

        let auth_policy = settings.get_string("cgka.auth_policy")
            .map(|policy| AuthorizationPolicy::from(policy))
            .unwrap_or(default.auth_policy);
        let scale = settings.get_bool("cgka.scale").unwrap_or(false);*/

        let groups = settings.get_array("cgka.groups")
            .map(|groups| groups.iter().map(|g| g.to_string()).collect())
            .unwrap_or(default.groups);
        let max_members = settings.get_int("cgka.max_members")
            .map(|max| max as usize)
            .unwrap_or(default.max_members);

        let paradigm = settings.get_string("paradigm.paradigm")
            .map(|paradigm| Paradigm::from(paradigm))
            .unwrap_or(default.paradigm);

        let proposals_per_commit = settings.get_int("paradigm.proposals_per_commit").unwrap_or(default.proposals_per_commit as i64) as usize;
        let invite_chance = settings.get_float("paradigm.invite_chance").unwrap_or(default.invite_chance);
        let remove_chance = settings.get_float("paradigm.remove_chance").unwrap_or(default.remove_chance);
        let update_chance = settings.get_float("paradigm.update_chance").unwrap_or(default.update_chance);

        if invite_chance + remove_chance + update_chance > 1.0 {
            return Err(String::from("The sum of \"invite_chance\", \"remove_chance\", \"update_chance\" cannot be greater than 1.0"));
        }

        Ok(Self {
            delivery_service,
            server_url,
            external_join,
            join_chance,
            message_chance,
            message_length_min,
            message_length_max,
            groups,
            max_members,

            behaviour,

            invite_chance,
            remove_chance,
            update_chance,
            paradigm,
            proposals_per_commit,

            mqtt_url,
        })
    }
}

impl Default for UserParameters {
    fn default() -> Self {
        Self {
            delivery_service: DSType::Request,
            server_url: Url::parse("http://localhost:8080").unwrap(),
            external_join: false,
            join_chance: 0.05,
            message_chance: 0.4,
            message_length_min: 200,
            message_length_max: 2000,
            behaviour: Behaviour::Independent(IndependentParameters::default()),
            groups: vec!["group_AAA".to_string()],
            max_members: 1000,
            invite_chance: 0.6,
            remove_chance: 0.1,
            update_chance: 0.3,
            paradigm: Paradigm::Commit,
            proposals_per_commit: 2,

            mqtt_url: "tcp://localhost:1883".to_string(),
        }
    }
}