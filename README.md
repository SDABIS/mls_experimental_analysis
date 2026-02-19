# MLS Emulation testbed

Testbed for experimental analysis of the Messaging Layer Security (MLS) protocol, as specified in [our paper](). We refer to our publication for definitions of terms employed in this project.

This project is divided into 2 folders:
- [*/emulated_client*](./emulated_client): Contains the Rust project for the emulated MLS client and its interaction with the Delivery Services. 
- [*/environment*](./environment): Contains scripts to build and deploy the simulation environment, as well as the configuration files

## Dependencies

- Rust (nightly)
- Docker

## Deployment

- Configure the simulation parameters in */environment/client/Settings.toml*. Each parameter and its possible values are explained in the configuration file.
    - IMPORTANT: Remember to set the URL of the HTTP Server and/or MQTT broker to the IP address of the machine where they are located, or to "localhost".
- Build the Docker environment: 
```
cd environment
./build.sh
```
IMPORTANT: It is necessary to execute the *build.sh* script every time the */environment/client/Settings.toml* configuration file is modified. Otherwise, the changes will not be applied.

- Use the *deploy.sh* script to launch the clients (*-c N*) and/or the server (*-s*). 
    - It is recommended that clients and server are located in different machines.
- To finish the simulation, remove the Docker swarm with the following command:
```
docker service rm mls-client mls-rendezvous
```

## Delivery Services

The testbed supports two different Delivery Services: an MQTT broker and a P2P GossipSub network. We recommend the use of the MQTT DS.

In order to enable the GossipSub DS, it is necessary to modify the *Settings.toml* file and set the parameter *ds* to "gossipsub". Then, the Rendezvous Server should also be enabled in the same network as the clients. This is done by uncommenting the "rendezvous" service in the [*/emulated_client*](./environment/client/docker-compose.yml). Make sure that the Rendezvous server is initialised before the clients.

## Analysis 

Each client generates a log file with its name in the folder *environment/client/logs*. These log files can be analysed and structured into a CSV using the *environment/log_scripts/log_parser.sh* script. Other executables in the same folder help in the creation of plots to visualize the results.

IMPORTANT: the *log_parser.sh* script will read every log file in the folder. Remember to delete the logs of previous executions so that they do not interfere.

## Implementation

Our implementation is based on [OpenMLS](https://github.com/openmls/openmls), a Rust implementation of the Messaging Layer Security (MLS) protocol, as specified in [RFC 9420](https://datatracker.ietf.org/doc/html/rfc9420). We apply very small modifications to OpenMLS, mostly to function interfaces. We also use their implementation of a Delivery Service (modules [ds](./emulated_client/delivery-service/ds/) and [ds-lib](./emulated_client/delivery-service/ds-lib/)) as a baseline for our Directory and Signaling Server.

Our main contribution is in the module [emulated_client](./emulated_client/emulated_client/), which autonomously acts as one or more MLS clients and sends and receives messages through the Delivery Service.  

For the implementation of the Delivery Services we employ the crates [rumqttc](https://crates.io/crates/rumqttc) and [rust-libp2p](https://github.com/libp2p/rust-libp2p).