// #[macro_use]
// extern crate clap;
// use clap::App;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use emulated_client::provider::openmls_rust_persistent_crypto::OpenMlsRustPersistentCrypto;
use emulated_client::provider::null_storage_provider::OpenMlsRustNullStorageCrypto;
use tracing::{Level};
use clap::{Arg, ArgAction, Command};
use config::Config;
use rumqttc::{AsyncClient, MqttOptions, NetworkOptions};
use url::Url;
use emulated_client::config::{Behaviour, DSType};
use emulated_client::config::user_parameters::UserParameters;
use emulated_client::client_agent::{orchestrated::OrchestratedClientAgent, independent::IndependentClientAgent, ClientAgent};
use emulated_client::pubsub::gossipsub_broker::GossipSubBroker;
use emulated_client::pubsub::gossipsub_updater::{GossipSubQueueMessage, GossipSubUpdater};
use emulated_client::pubsub::mqtt_broker::MqttBroker;
use emulated_client::pubsub::mqtt_updater::{MQTTQueueMessage, MqttUpdater};
use emulated_client::user::{DeliveryService, User};
use jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub fn create_client_agent(ds: DeliveryService, user_parameters: UserParameters, name: String) 
-> (Arc<Mutex<User<OpenMlsRustPersistentCrypto>>>, Box<dyn ClientAgent<OpenMlsRustPersistentCrypto> + Send>) {

    let crypto = OpenMlsRustPersistentCrypto::default();

    match &user_parameters.behaviour {
        Behaviour::Orchestrated(orchestrator_params) => {
            let (sender, receiver) = tokio::sync::mpsc::channel::<String>(100);

            let user = User::new_orchestrated(crypto, name.clone(), user_parameters.server_url.clone(), ds.clone(), sender, orchestrator_params.clone());
            let user = Arc::new(Mutex::new(user));
            let agent = OrchestratedClientAgent::new(name.clone(), Arc::clone(&user), user_parameters.clone(), receiver);
            (user, Box::new(agent) as Box<dyn ClientAgent<_> + Send>)
        },
        Behaviour::Independent(_) => {
            let user = User::new_independent(crypto, name.clone(), user_parameters.server_url.clone(), ds.clone());
            let user = Arc::new(Mutex::new(user));
            let agent = IndependentClientAgent::new(name.clone(), Arc::clone(&user), user_parameters.clone());

            (user, Box::new(agent) as Box<dyn ClientAgent<_> + Send>)
        }
    }
}

pub fn create_user_from_ds(user_parameters: UserParameters, replicas: usize, username: String) -> Vec<Box<dyn ClientAgent<OpenMlsRustPersistentCrypto> + Send>> {
    match user_parameters.delivery_service {
        DSType::Request => {
            (0..replicas).map(|i| {
                let name = format!("{}_{}",username, i);

                let ds = DeliveryService::Request;
                create_client_agent(ds, user_parameters.clone(), name).1
            }).collect()
        },
        DSType::PubSubMQTT(ref url_str) => {
            let (tx, rx) = tokio::sync::mpsc::channel::<MQTTQueueMessage>(100);

            
            let url = Url::parse(&url_str).unwrap();
            let mut mqtt_options = MqttOptions::new(username.clone(), url.host_str().unwrap(), url.port().unwrap());
            mqtt_options.set_keep_alive(Duration::from_secs(10));
            mqtt_options.set_max_packet_size(1024 * 1024 * 1024, 1024 * 1024 * 1024);
            let (async_client, mut event_loop) = AsyncClient::new(mqtt_options, 200);
            let mut network_options = NetworkOptions::new();
            network_options.set_connection_timeout(60);
            event_loop.set_network_options(network_options);

            let (users, client_agents): 
            (Vec<(String,Arc<Mutex<User<_>>>)>, Vec<Box<dyn ClientAgent<_> + Send>>) = 
            (0..replicas).map(|i| {
                let name = format!("{}_{}",username, i);


                let broker = MqttBroker::new_from_client(name.clone(), async_client.clone(), tx.clone());
                let broker = Arc::new(Mutex::new(broker));
                let ds = DeliveryService::PubSubMQTT(Arc::clone(&broker));

                let (user, client_agent) = create_client_agent(ds, user_parameters.clone(), name.clone());
                ((name.clone(), user), client_agent)

            }).unzip();

            let users_thread: HashMap<String, Arc<Mutex<User<_>>>> = users
                .iter().map(|(name, u)| (name.clone(), Arc::clone(u))).collect();

                thread::spawn(move || {
                    let mut mqtt_updater = MqttUpdater::new(users_thread, rx, event_loop, async_client);
                    
                    mqtt_updater.run();
                });

            client_agents
        }
        DSType::GossipSub(ref config) => {
            let (tx, rx) = tokio::sync::mpsc::channel::<GossipSubQueueMessage>(100);

            let (users, client_agents):
            (Vec<(String,Arc<Mutex<User<_>>>)>, Vec<Box<dyn ClientAgent<_> + Send>>) = 
            (0..replicas).map(|i| {
                let name = format!("{}_{}",username, i);

                let broker = GossipSubBroker::new(name.clone(), tx.clone());
                let broker = Arc::new(Mutex::new(broker));
                let ds = DeliveryService::GossipSub(Arc::clone(&broker), config.directory.clone());

                let (user, client_agent) = create_client_agent(ds, user_parameters.clone(), name.clone());

                ((name.clone(), user), client_agent)
            }).unzip();

            let users_thread: HashMap<String, Arc<Mutex<User<_>>>> = users
                .iter().map(|(name, u)| (name.clone(), Arc::clone(u))).collect();

            let mut gossipsub_updater = GossipSubUpdater::new(users_thread, config.clone(), rx);

            thread::spawn(move || {
                gossipsub_updater.run();
            });

            tracing::info!("Bootstrapping P2P network...");
            thread::sleep(Duration::from_secs(30));


            client_agents
        }
    }
}

fn main() {
    let matches = Command::new("OpenMLS Emulated Client")
        .version("0.1.0")
        .author("David Soler")
        .about("PoC MLS Delivery Service")
        .arg(
            Arg::new("name")
                .short('n')
                .long("name")
                .action(ArgAction::Append),
        ).arg(
        Arg::new("config-file")
            .short('c')
            .long("configuration file")
            .action(ArgAction::Append),
    )
        .get_matches();

    let name = matches.get_one::<String>("name").unwrap_or(&"User_1".to_string()).clone();

    let config_filename = matches.get_one::<String>("config-file").cloned().unwrap_or("emulated_client/resources/Settings.toml".to_string());
    let settings = Config::builder()
        // Add in `./Settings.toml`
        .add_source(config::File::with_name(&config_filename))
        // Add in settings from the environment
        .add_source(config::Environment::with_prefix("CGKA"))
        .build()
        .unwrap();

    let replicas = settings.get_int("meta.replicas").unwrap_or(1) as usize;

    let parameters = UserParameters::new_from_settings(&settings).expect("Error parsing Configuration file");
    println!("{:?}", parameters);

    let subscriber = tracing_subscriber::fmt().without_time().with_target(false).with_writer(std::io::stdout).with_max_level(Level::INFO).finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let mut threads = vec![];

    let users = create_user_from_ds(parameters.clone(), replicas, name.clone());
    for mut client_agent in users {
        
        let thread = thread::Builder::new()
        //.stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tracing::span!(Level::INFO, "agent", user = client_agent.username()).in_scope(|| {
                client_agent.run();
            });
        }).unwrap();

        thread::sleep(Duration::from_millis(500));
        threads.push(thread);
    }

    for thread in threads {
        thread.join().expect("Error with thread");
    }


}
