//! # The OpenMLS Delivery Service (DS).
//!
//! This is a minimal implementation of 2.3. Delivery Service in
//! [The MLS Architecture](https://messaginglayersecurity.rocks/mls-architecture/draft-ietf-mls-architecture.html).
//! It is used for end-to-end testing of OpenMLS and can be used by other
//! implementations. However it should never be used in any sort of production
//! environment.
//!
//! Because the infrastructure description doesn't give a lot of guidelines on
//! the design of the DS we take a couple of deliberate design decisions here:
//! * The DS does not know about groups.
//! * Clients have to send a list of clients (group members) along with each
//!   message for the DS to know where to send the message.
//! * The DS stores and delivers key packages.
//!
//! This is a very basic delivery service that allows to register clients and
//! send messages to MLS groups.
//! Note that there are a lot of limitations to this service:
//! * No persistence layer such that all information gets lost when the process
//!   shuts down.
//! * No authentication for clients.
//! * Key packages can't be updated, changed or deleted at the moment.
//! * Messages lost in transit are gone.
//!
//! **⚠️ DON'T EXPECT ANY SECURITY OR PRIVACY FROM THIS!**
//!
//! The server always listens on localhost and should be run behind a TLS server
//! if accessible on the public internet.
//!
//! The DS returns a list of messages queued for the client in all groups they
//! are part of.

use actix_web::{get, post, web, web::Payload, App, HttpRequest, HttpServer, Responder};
use clap::{ArgAction, Command};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::str::from_utf8;
use std::sync::Mutex;
use serde::Deserialize as SerdeDeserialize;
use tls_codec::{Deserialize, Serialize, TlsSliceU32, TlsVecU32};

use ds_lib::*;
use openmls::prelude::*;
use openmls::prelude::group_info::VerifiableGroupInfo;

/// The DS state.
/// It holds a list of clients and their information.
#[derive(Default, Debug)]
pub struct DsData {
    // (ClientIdentity, ClientInfo)
    clients: Mutex<HashMap<Vec<u8>, ClientInfo>>,
    group_info_mutex: bool,
    // (group_id, epoch)
    groups: Mutex<HashMap<Vec<u8>, u64>>,
    group_info: Mutex<HashMap<Vec<u8>, Option<VerifiableGroupInfo>>>,
}

#[derive(Debug, Clone, SerdeDeserialize)]
pub struct UsernameQuery {
    username: String,
}

macro_rules! unwrap_item {
    ( $e:expr ) => {
        match $e {
            Ok(x) => x,
            Err(_) => return actix_web::HttpResponse::PartialContent().finish(),
        }
    };
}

macro_rules! unwrap_data {
    ( $e:expr ) => {
        match $e {
            Ok(x) => x,
            Err(_) => return actix_web::HttpResponse::InternalServerError().finish(),
        }
    };
}

// === API ===

/// Registering a new client takes a serialised `ClientInfo` object and returns
/// a simple "Welcome {client name}" on success.
/// An HTTP conflict (409) is returned if a client with this name exists
/// already.
#[post("/clients/register")]
async fn register_client(mut body: Payload, data: web::Data<DsData>) -> impl Responder {
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&unwrap_item!(item));
    }
    let info = match ClientInfo::tls_deserialize(&mut &bytes[..]) {
        Ok(i) => i,
        Err(_) => {
            log::error!("Invalid payload for /clients/register\n{:?}", bytes);
            return actix_web::HttpResponse::BadRequest().finish();
        }
    };
    log::debug!("Registering client: {}", info.client_name);

    let mut clients = unwrap_data!(data.clients.lock());
    let client_name = info.client_name.clone();
    let old = clients.insert(info.id.clone(), info);
    if old.is_some() {
        return actix_web::HttpResponse::Conflict().finish();
    }

    actix_web::HttpResponse::Ok().body(format!("Welcome {client_name}!\n"))
}

/// Returns a list of clients with their names and IDs.
#[get("/clients/list")]
async fn list_clients(_req: HttpRequest, data: web::Data<DsData>) -> impl Responder {
    let clients = unwrap_data!(data.clients.lock());

    // XXX: we could encode while iterating to be less wasteful.
    let clients: TlsVecU32<ClientInfo> = clients
        .values()
        .cloned()
        .collect::<Vec<ClientInfo>>()
        .into();
    let mut out_bytes = Vec::new();
    if clients.tls_serialize(&mut out_bytes).is_err() {
        return actix_web::HttpResponse::InternalServerError().finish();
    };

    log::debug!("Listing clients: {:?}", clients.len());

    actix_web::HttpResponse::Ok().body(out_bytes)
}

/// Resets the server state.
#[get("/reset")]
async fn reset(_req: HttpRequest, data: web::Data<DsData>) -> impl Responder {
    log::debug!("Resetting server");
    let mut clients = unwrap_data!(data.clients.lock());
    let mut groups = unwrap_data!(data.groups.lock());
    clients.clear();
    groups.clear();
    actix_web::HttpResponse::Ok().finish()
}

/// Get the list of key packages for a given client `{id}`.
/// This returns a serialised vector of `ClientKeyPackages` (see the `ds-lib`
/// for details).
#[get("/clients/key_packages/{id}")]
async fn get_key_packages(path: web::Path<String>, data: web::Data<DsData>) -> impl Responder {
    let clients = unwrap_data!(data.clients.lock());

    let id = match base64::decode_config(path.into_inner(), base64::URL_SAFE) {
        Ok(v) => v,
        Err(_) => return actix_web::HttpResponse::BadRequest().finish(),
    };
    log::debug!("Getting key packages for {:?}", id);

    let client = match clients.get(&id) {
        Some(c) => c,
        None => return actix_web::HttpResponse::NoContent().finish(),
    };
    actix_web::HttpResponse::Ok().body(unwrap_data!(client.key_packages.tls_serialize_detached()))
}

/// Publish key packages for a given client `{id}`.
#[post("/clients/key_packages/{id}")]
async fn publish_key_packages(
    path: web::Path<String>,
    mut body: Payload,
    data: web::Data<DsData>,
) -> impl Responder {
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&unwrap_item!(item));
    }

    let mut clients = unwrap_data!(data.clients.lock());

    let id = match base64::decode_config(path.into_inner(), base64::URL_SAFE) {
        Ok(v) => v,
        Err(_) => return actix_web::HttpResponse::BadRequest().finish(),
    };
    log::debug!("Add key package for {}", from_utf8(id.as_slice()).unwrap());

    let client = match clients.get_mut(&id) {
        Some(client) => client,
        None => return actix_web::HttpResponse::NotFound().finish(),
    };

    let key_packages = match ClientKeyPackages::tls_deserialize(&mut &bytes[..]) {
        Ok(ckp) => ckp,
        Err(_) => {
            log::error!(
                "Invalid payload for /clients/key_packages/{:?}\n{:?}",
                id,
                bytes
            );
            return actix_web::HttpResponse::BadRequest().finish();
    }
    };

    key_packages
        .0
        .iter()
        .map(|(b, kp)| (b.clone(), kp.clone()))
        .for_each(|value| client.key_packages.0.push(value));

    actix_web::HttpResponse::Ok().finish()
}

/// Consume a key package for a given client `{id}`.
/// This returns a serialised `KeyPackage` (see the `ds-lib`
/// for details).
#[get("/clients/key_package/{id}")]
async fn consume_key_package(path: web::Path<String>, data: web::Data<DsData>) -> impl Responder {
    let mut clients = unwrap_data!(data.clients.lock());

    let id = match base64::decode_config(path.into_inner(), base64::URL_SAFE) {
        Ok(v) => v,
        Err(_) => return actix_web::HttpResponse::BadRequest().finish(),
    };
    log::debug!("Consuming key package for {:?}", id);

    let key_package = match clients.get_mut(&id) {
        Some(c) => match c.consume_kp() {
            Ok(kp) => kp,
            Err(e) => {
                log::debug!("Error consuming key package: {}", e);
                return actix_web::HttpResponse::NoContent().finish();
            }
        },
        None => return actix_web::HttpResponse::NoContent().finish(),
    };

    actix_web::HttpResponse::Ok().body(unwrap_data!(key_package.tls_serialize_detached()))
}

#[get("/groups/group_info/{id}")]
async fn get_group_info(path: web::Path<String>, params: web::Query<UsernameQuery>, data: web::Data<DsData>) -> impl Responder {
    let mut groups = unwrap_data!(data.group_info.lock());

    let id = match base64::decode_config(path.into_inner(), base64::URL_SAFE) {
        Ok(v) => v,
        Err(_) => return actix_web::HttpResponse::BadRequest().finish(),
    };

    let username = params.username.clone();
    log::debug!("{username} requests group info of {:?}", std::str::from_utf8(id.as_slice()).unwrap());

    let group_info = match groups.get(&id) {
        Some(c) => {
            match c {
                Some(gi) => gi.clone(),

                // Group Info has already been requested
                None => {
                    log::debug!("Group info Forbidden to {:?}", std::str::from_utf8(id.as_slice()).unwrap());
                    return actix_web::HttpResponse::Forbidden().finish()
                },
            }
        },
        None => {
            if data.group_info_mutex {
                groups.insert(id, None);
            }
            return actix_web::HttpResponse::NoContent().finish()
        },
    };

    if data.group_info_mutex {
        groups.insert(id, None);
    }
    actix_web::HttpResponse::Ok().body(unwrap_data!(group_info.tls_serialize_detached()))
}

#[get("/groups/peek/{id}")]
async fn group_exists(path: web::Path<String>, data: web::Data<DsData>) -> impl Responder {
    log::info!("Checking if group exists");
    let groups = unwrap_data!(data.group_info.lock());

    let id = match base64::decode_config(path.into_inner(), base64::URL_SAFE) {
        Ok(v) => v,
        Err(_) => return actix_web::HttpResponse::BadRequest().finish(),
    };

    let response = groups.get(&id).is_some();
    actix_web::HttpResponse::Ok().body(response.to_string())
}

#[post("/groups/group_info/{id}")]
async fn post_group_info(
    path: web::Path<String>,
    params: web::Query<UsernameQuery>,
    mut body: Payload,
    data: web::Data<DsData>
) -> impl Responder {
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&unwrap_item!(item));
    }

    let mut groups = unwrap_data!(data.group_info.lock());

    let id = match base64::decode_config(path.into_inner(), base64::URL_SAFE) {
        Ok(v) => v,
        Err(_) => return actix_web::HttpResponse::BadRequest().finish(),
    };

    let username = params.username.clone();
    log::debug!("Add group info for {:?} by {}", std::str::from_utf8(id.as_slice()).unwrap(), username);

    let group_info = match VerifiableGroupInfo::tls_deserialize(&mut &bytes[..]) {
        Ok(gi) => gi,
        Err(_) => {
            log::error!(
                    "Invalid payload for /clients/group_info/{:?}\n{:?}",
                    id,
                    bytes
                );
            return actix_web::HttpResponse::BadRequest().finish();
        }
    };

    groups.insert(id, Some(group_info));

    actix_web::HttpResponse::Ok().finish()
}

/// Send a welcome message to a client.
/// This takes a serialised `Welcome` message and stores the message for all
/// clients in the welcome message.
#[post("/send/welcome")]
async fn send_welcome(mut body: Payload, data: web::Data<DsData>) -> impl Responder {
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&unwrap_item!(item));
    }
    let welcome_msg = unwrap_data!(GroupMessage::tls_deserialize(&mut &bytes[..]));
    let welcome = welcome_msg.clone().msg.into_welcome().unwrap();
    log::debug!("Storing welcome message: {:?}", welcome_msg);

    let mut clients = unwrap_data!(data.clients.lock());
    for secret in welcome.secrets().iter() {
        let key_package_hash = &secret.new_member();
        for (_client_name, client) in clients.iter_mut() {
            match client
                .reserved_key_pkg_hash
                .take(key_package_hash.as_slice())
            {
                Some(_kp_hash) => {
                    client.welcome_queue.push(welcome_msg);
                    return actix_web::HttpResponse::Ok().finish();
                }
                None => continue,
            };
        }
    }
    actix_web::HttpResponse::NoContent().finish()
}

/// Send an MLS message to a set of clients (group).
/// This takes a serialised `GroupMessage` and stores the message for each
/// client in the recipient list.
/// If a handshake message is sent with an epoch smaller or equal to another
/// handshake message this DS has seen, a 409 is returned and the message is not
/// processed.
#[post("/send/message")]
async fn msg_send(mut body: Payload, data: web::Data<DsData>) -> impl Responder {
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&unwrap_item!(item));
    }
    let group_msg = unwrap_data!(GroupMessage::tls_deserialize(&mut &bytes[..]));
    log::debug!("Storing group message: {:?}", group_msg);

    let mut clients = unwrap_data!(data.clients.lock());
    let mut groups = unwrap_data!(data.groups.lock());

    let protocol_msg: ProtocolMessage = ProtocolMessage::try_from(group_msg.msg.clone()).unwrap();

    if protocol_msg.is_handshake_message() {
        let epoch = protocol_msg.epoch().as_u64();
        let group_id = protocol_msg.group_id().as_slice();
        if let Some(&group_epoch) = groups.get(group_id) {
            if group_epoch > epoch {
                log::debug!("CONFLICT BECAUSE OF EPOCH: {} > {}", group_epoch, epoch);
                return actix_web::HttpResponse::Conflict().finish();
            }
            // Update server state to the latest epoch.
            let old_value = groups.insert(group_id.to_vec(), epoch);
            if old_value.is_none() {
                return actix_web::HttpResponse::InternalServerError().finish();
            }
        } else {
            // We haven't seen this group_id yet. Store it.
            let old_value = groups.insert(group_id.to_vec(), epoch);
            if old_value.is_some() {
                return actix_web::HttpResponse::InternalServerError().finish();
            }
        }
    }

    for recipient in group_msg.recipients.iter() {
        let client = match clients.get_mut(recipient.as_slice()) {
            Some(client) => client,
            None => return actix_web::HttpResponse::NotFound().finish(),
        };
        client.msgs.push(group_msg.clone());
    }
    let client = match clients.get_mut(group_msg.sender.as_slice()) {
        Some(client) => client,
        None => return actix_web::HttpResponse::NotFound().finish(),
    };
    client.msgs.push(group_msg.clone());

    actix_web::HttpResponse::Ok().finish()
}

/// Receive all messages stored for the client `{id}`.
/// This returns a serialised vector of `Message`s (see the `ds-lib` for
/// details) the DS has stored for the given client.
/// The messages are deleted on the DS when sent out.
#[get("/recv/{id}")]
async fn msg_recv(path: web::Path<String>, data: web::Data<DsData>) -> impl Responder {
    let mut clients = unwrap_data!(data.clients.lock());

    let id = match base64::decode_config(path.into_inner(), base64::URL_SAFE) {
        Ok(v) => v,
        Err(_) => return actix_web::HttpResponse::BadRequest().finish(),
    };
    log::debug!("Getting messages for client {:?}", std::str::from_utf8(id.as_slice()).unwrap());
    let client = match clients.get_mut(&id) {
        Some(client) => client,
        None => return actix_web::HttpResponse::NotFound().finish(),
    };

    let mut out: Vec<GroupMessage> = Vec::new();
    let mut welcomes: Vec<GroupMessage> = client.welcome_queue.drain(..).collect();
    out.append(&mut welcomes);
    let mut msgs: Vec<GroupMessage> = client.msgs.drain(..).collect();
    out.append(&mut msgs);

    match TlsSliceU32(&out).tls_serialize_detached() {
        Ok(out) => actix_web::HttpResponse::Ok().body(out),
        Err(_) => actix_web::HttpResponse::InternalServerError().finish(),
    }
}

// === Main function driving the DS ===

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    pretty_env_logger::init();
    //env_logger::init();
    // Configure App and command line arguments.
    let matches = Command::new("OpenMLS DS")
        .version("0.1.0")
        .author("OpenMLS Developers")
        .about("PoC MLS Delivery Service")
        .arg(
            clap::Arg::new("port")
                .short('p')
                .long("port")
                .value_name("port")
                .help("Sets a custom port number"),
        ).arg(
            clap::Arg::new("group_info_mutex")
            .short('m')
            .long("group info mutex")
            .action(ArgAction::SetTrue),
    )
        .get_matches();
    // Set default port or use port provided on the command line.
    let port = matches.get_one("port").cloned().unwrap_or("8080".to_string());

    let ip = "0.0.0.0";
    let addr = format!("{ip}:{port}");
    log::info!("Listening on: {}", addr);

    let group_info_mutex = matches.get_flag("group_info_mutex");

    // The data this app operates on.
    let mut data = DsData::default();
    data.group_info_mutex = group_info_mutex;

    let data = web::Data::new(data);

    // Start the server.
    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .service(register_client)
            .service(list_clients)
            .service(publish_key_packages)
            .service(get_key_packages)
            .service(consume_key_package)
            .service(group_exists)
            .service(get_group_info)
            .service(post_group_info)
            .service(send_welcome)
            .service(msg_recv)
            .service(msg_send)
            .service(reset)
    })
    .bind(addr)?
    .run()
    .await
}
