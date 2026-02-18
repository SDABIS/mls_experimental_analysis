extern crate emulated_client;
extern crate mls_rs;
extern crate mls_rs_crypto_openssl;
extern crate openmls_traits;
extern crate rand;
extern crate cpu_time;

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Copyright by contributors to this project.
// SPDX-License-Identifier: (Apache-2.0 OR MIT)
use rand::rng;
use mls_rs::{
    Group,
    client_builder::MlsConfig,
    error::MlsError,
    identity::{
        basic::{BasicCredential, BasicIdentityProvider},
        SigningIdentity,
    },
    CipherSuite, CipherSuiteProvider, Client, CryptoProvider, ExtensionList,
};

const CIPHERSUITE: CipherSuite = CipherSuite::CURVE25519_AES128;

fn make_client<P: CryptoProvider + Clone>(
    crypto_provider: P,
    name: &str,
) -> Result<Client<impl MlsConfig>, MlsError> {
    let cipher_suite = crypto_provider.cipher_suite_provider(CIPHERSUITE).unwrap();

    // Generate a signature key pair.
    let (secret, public) = cipher_suite.signature_key_generate().unwrap();

    // Create a basic credential for the session.
    // NOTE: BasicCredential is for demonstration purposes and not recommended for production.
    // X.509 credentials are recommended.
    let basic_identity = BasicCredential::new(name.as_bytes().to_vec());
    let signing_identity = SigningIdentity::new(basic_identity.into_credential(), public);

    Ok(Client::builder()
        .identity_provider(BasicIdentityProvider)
        .crypto_provider(crypto_provider)
        .signing_identity(signing_identity, secret, CIPHERSUITE)
        .build())
}

use std::cell::RefCell;
use std::cmp::min;
use cpu_time::ProcessTime;
use rand::{Rng};

const MAX_ACTIVE_COMMITTERS: usize = 10;
const MAX_ACTIVE_PROCESSERS: usize = 100;
const MAX_GROUP_MEMBERS: usize = 10000;

fn main() {

    let crypto_provider = mls_rs_crypto_openssl::OpensslCryptoProvider::default();

    let identities: Vec<_> = (0..MAX_GROUP_MEMBERS).map(|i| {
        make_client(crypto_provider.clone(), &format!("User_{}", i)).unwrap()
    }).collect();

    let initial_group = identities[0].create_group(ExtensionList::default(), Default::default(), None).unwrap();

    let active_members = RefCell::new(vec![]);


    let mut current_members = 1;

    //let seed = initial_group.transcript_hash();
    //let seed = seed.as_slice();
    //let mut seed_array = [0u8; 32];
    //seed_array[..seed.len().min(32)].copy_from_slice(&seed[..seed.len().min(32)]);
    //let mut rng= StdRng::from_seed(seed_array);
    
    let mut rng= rng();
    active_members.borrow_mut().push((0, initial_group));


    while current_members < MAX_GROUP_MEMBERS {

        //println!("Current members: {}", current_members);
        let chosen_committer = rng.random_range(0..min(current_members, MAX_ACTIVE_COMMITTERS));
        let new_member_identity = identities.get(current_members).unwrap();

        let now = ProcessTime::now();

        let commit = {

            let key_package =
                new_member_identity.generate_key_package_message(Default::default(), Default::default(), None).unwrap();

            let mut active_members = active_members.borrow_mut();
            let (_, committer_group) = active_members.get_mut(chosen_committer).unwrap();

            // Alice issues a commit that adds Bob to the group.
            let commit = committer_group
                .commit_builder()
                .add_member(key_package).unwrap()
                .build().unwrap();
            committer_group.apply_pending_commit().unwrap();

           /*let seed = committer_group.transcript_hash();
            let seed = seed.as_slice();
            let mut seed_array = [0u8; 32];
            seed_array[..seed.len().min(32)].copy_from_slice(&seed[..seed.len().min(32)]);

            rng = StdRng::from_seed(seed_array); */
            

            commit
        };

        let committer_id = {
            let active_members = active_members.borrow();
            let (committer_id, _committer_group) = active_members.get(chosen_committer).unwrap();
            committer_id.clone()
        };
        let mut active_members = active_members.borrow_mut();

        for (id, group) in active_members.iter_mut() {
            if *id == committer_id {
                continue;
            }
            group.process_incoming_message(commit.commit_message.clone()).unwrap();
        };

        let sample = rng.random_range(0..current_members);
        // If committers are not yet full, become committer and processor
        if current_members <= MAX_ACTIVE_COMMITTERS {
            let (new_group, _) = new_member_identity.join_group(None, &commit.welcome_messages[0], None).unwrap();
            active_members.push((current_members, new_group));
        }

        // If committers are full but processors are not yet full, chance to become committer. If not, become processor
        else if current_members <= MAX_ACTIVE_PROCESSERS {
            let (new_group, _) = new_member_identity.join_group(None, &commit.welcome_messages[0], None).unwrap();
            active_members.push((current_members, new_group));

            // Become committer
            if sample <= MAX_ACTIVE_COMMITTERS {
                active_members.swap(sample, current_members -1);
            }
            // Does not become committer, but becomes processor
            //else {}
        }
        // If both committers and processors are full, chance to become any of them
        else if sample < MAX_ACTIVE_PROCESSERS {
            let (new_group, _) = new_member_identity.join_group(None, &commit.welcome_messages[0], None).unwrap();
            active_members.push((current_members, new_group));
            let removed_element = active_members.swap_remove(sample);

            drop(removed_element);
        }

        current_members += 1;

        let elapsed = now.elapsed().as_millis();
        if current_members% 10 == 0 {
            println!("Members: {} - Time: {} ms", current_members, elapsed);
        }
    }

}
