extern crate emulated_client;
extern crate openmls;
extern crate openmls_traits;
extern crate rand;
extern crate criterion;
extern crate cpu_time;

use std::cell::RefCell;
use std::cmp::min;
use cpu_time::ProcessTime;
use criterion::{BatchSize, Criterion};
use openmls::prelude::{RatchetTreeIn, GroupId, MlsGroup, MlsGroupCreateConfig, MlsGroupJoinConfig, MlsMessageBodyIn, MlsMessageIn, MlsMessageOut, ProcessedMessageContent, StagedWelcome};
use openmls_traits::types::Ciphersuite;
use emulated_client::{identity::Identity};
use emulated_client::openmls_rust_persistent_crypto::OpenMlsRustPersistentCrypto;
use rand::{Rng, SeedableRng};
use rand::prelude::StdRng;
use openmls::prelude::tls_codec::{Deserialize, Serialize};
use openmls_traits::OpenMlsProvider;

const CIPHERSUITE: Ciphersuite = Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;
const GROUP_NAME: &str = "benchmark_group";

const MAX_ACTIVE_COMMITTERS: usize = 200;
const MAX_ACTIVE_PROCESSERS: usize = 200;
const MAX_GROUP_MEMBERS: usize = 10000;

fn message_out_to_in(mls_message_out: MlsMessageOut) -> MlsMessageIn {
    let serialized_out = mls_message_out
        .tls_serialize_detached().unwrap();
    MlsMessageIn::tls_deserialize(&mut serialized_out.as_slice()).unwrap()
}
fn process_protocol_message(crypto: &OpenMlsRustPersistentCrypto, group: &mut MlsGroup, message: MlsMessageIn, merge: bool)
{
    let message = message.try_into_protocol_message().unwrap();
    let processed_message = group.process_message(crypto, message.clone()).unwrap();

    match processed_message.into_content() {

        ProcessedMessageContent::StagedCommitMessage(commit_ptr) => {
            if merge {
                group.merge_staged_commit(crypto, *commit_ptr).unwrap();
            }
        }

        _ => {}
    }
}
fn process_invitation(crypto: &OpenMlsRustPersistentCrypto, welcome: MlsMessageIn, tree: RatchetTreeIn) -> MlsGroup {

    let welcome = match welcome.extract() {
        MlsMessageBodyIn::Welcome(w) => w,
        _ => {panic!()}
    };

    //let identity = self.identity.clone();
    /*for secret in welcome.secrets().iter() {
        let key_package_hash = &secret.new_member();
        if self.identity.kp.contains_key(key_package_hash.as_slice()) {
            self.identity.kp.remove(key_package_hash.as_slice());
        }
    }*/

    let group_config = MlsGroupJoinConfig::builder()
        .use_ratchet_tree_extension(false)
        .sender_ratchet_configuration(
            openmls::prelude::SenderRatchetConfiguration::new(
                10000u32,10000u32,
            )
        )
        .build();

    let staged_welcome = StagedWelcome::new_from_welcome(crypto, &group_config, welcome, Some(tree)).unwrap();

    let mls_group = staged_welcome.into_group(crypto)
        .unwrap();

    mls_group
}

fn group_benchmarks(c: &mut Criterion) {

    let crypto = OpenMlsRustPersistentCrypto::default();

    let mut active_members = RefCell::new(vec![]);

    let identities = (0..MAX_GROUP_MEMBERS).map(|i| {
        Identity::new(
            CIPHERSUITE,
            &crypto,
            format!("user_{}", i).as_bytes()
        )
    }).collect::<Vec<Identity>>();

    let group_config = MlsGroupCreateConfig::builder()
        .use_ratchet_tree_extension(false)
        .sender_ratchet_configuration(
            openmls::prelude::SenderRatchetConfiguration::new(
                10000u32,10000u32,
            )
        )
        .build();
    let initial_group = MlsGroup::new_with_group_id(
        &crypto,
        identities[0].signer(),
        &group_config,
        GroupId::from_slice(GROUP_NAME.as_bytes()),
        identities[0].credential_with_key().clone(),
    )
        .expect("Failed to create MlsGroup");

    let mut current_members = 1;

    let seed = initial_group.transcript_hash();
    let seed = seed.as_slice();
    let mut seed_array = [0u8; 32];
    seed_array[..seed.len().min(32)].copy_from_slice(&seed[..seed.len().min(32)]);

    let mut rng= StdRng::from_seed(seed_array);
    active_members.borrow_mut().push((0, initial_group));


    while current_members < MAX_GROUP_MEMBERS {



        //println!("Current members: {}", current_members);
        let chosen_committer = rng.random_range(0..min(current_members, MAX_ACTIVE_COMMITTERS));
        let new_member_identity = identities.get(current_members).unwrap();

        let committer_id = {
            let active_members = active_members.borrow();
            let (committer_id, _) = active_members.get(chosen_committer).unwrap();
            committer_id.clone()
        };
        let committer_identity = identities.get(committer_id).unwrap();


        if current_members % 100 == 0 {
            c.bench_function(
                &format!("Commit [size={}]", current_members),
                |b| {
                    b.iter(|| {

                        let mut active_members = active_members.borrow_mut();
                        let (_, committer_group) = active_members.get_mut(chosen_committer).unwrap();
                        let (_message_out, _welcome, _) = committer_group.add_members(
                            &crypto,
                            committer_identity.signer(),
                            &[new_member_identity.key_packages().get(0).cloned().unwrap()],
                        ).expect("Failed to add member");

                        committer_group.clear_pending_commit(crypto.storage()).unwrap()
                    });
                }
            );
            /*c.bench_function(
                &format!("Welcome [size={}]", current_members),
                |b| {
                    b.iter_batched(
                        || {
                            let mut active_members = active_members.borrow_mut();
                            let (committer_id, committer_group) = active_members.get_mut(chosen_committer).unwrap();
                            let committer_identity = identities.get(*committer_id).unwrap();

                            let (_, welcome, _) = committer_group.add_members(
                                &crypto,
                                committer_identity.signer(),
                                &[new_member_identity.key_packages().get(0).cloned().unwrap()],
                            ).expect("Failed to add member");

                            committer_group.clear_pending_commit(crypto.storage()).unwrap();
                            let welcome = message_out_to_in(welcome);
                            let tree = committer_group.export_ratchet_tree();
                            (welcome, tree)
                        },
                        |(welcome, tree )| {
                            process_invitation(&crypto, welcome, tree.into());
                        },
                        BatchSize::SmallInput
                    );
                }
            );*/

            c.bench_function(
                &format!("Process [size={}]", current_members),
                |b| {
                    b.iter_batched(
                        || {
                            let mut active_members = active_members.borrow_mut();
                            let (committer_id, committer_group) = active_members.get_mut(chosen_committer).unwrap();
                            let committer_identity = identities.get(*committer_id).unwrap();

                            let (message_out, _, _) = committer_group.add_members(
                                &crypto,
                                committer_identity.signer(),
                                &[new_member_identity.key_packages().get(0).cloned().unwrap()],
                            ).expect("Failed to add member");

                            committer_group.clear_pending_commit(crypto.storage()).unwrap();
                            let message_in = message_out_to_in(message_out);

                            let chosen_processer = {
                                let mut rng = rand::rng();
                                let mut sample = rng.random_range(
                                    0..min(current_members, MAX_ACTIVE_PROCESSERS)
                                );
                                while sample == chosen_committer {
                                    sample = rng.random_range(
                                        0..min(current_members, MAX_ACTIVE_PROCESSERS)
                                    )
                                }
                                //let (id, _) = active_members.get(sample).unwrap();

                                sample
                            };
                            (message_in, chosen_processer)
                        },
                        |(message_in, chosen_processer) | {
                            let mut active_members = active_members.borrow_mut();
                            let (_processer_id, group) = active_members.get_mut(chosen_processer).unwrap();

                            process_protocol_message(&crypto, group, message_in, false);
                        },
                        BatchSize::SmallInput
                    );
                }
            );
        }

        let now = ProcessTime::now();

        let (message_in, welcome, tree) = {

            let mut active_members = active_members.borrow_mut();
            let (_, committer_group) = active_members.get_mut(chosen_committer).unwrap();

            //println!("Chosen committer: {}, with ID: {}", chosen_committer, committer_id);
            let (message_out, welcome, _) = committer_group.add_members(
                &crypto,
                committer_identity.signer(),
                &[new_member_identity.key_packages().get(0).cloned().unwrap()],
            ).expect("Failed to add member");
            committer_group.merge_pending_commit(&crypto).unwrap();

            let seed = committer_group.transcript_hash();
            let seed = seed.as_slice();
            let mut seed_array = [0u8; 32];
            seed_array[..seed.len().min(32)].copy_from_slice(&seed[..seed.len().min(32)]);

            rng = StdRng::from_seed(seed_array);

            let message_in = message_out_to_in(message_out);
            let welcome = message_out_to_in(welcome);
            let tree = committer_group.export_ratchet_tree();

            (message_in, welcome, tree)
        };

        let new_group = process_invitation(&crypto, welcome, tree.into());

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
            process_protocol_message(&crypto, group, message_in.clone(), true);
        };

        let sample = rng.random_range(0..current_members);
        // If committers are not yet full, become committer and processor
        if current_members <= MAX_ACTIVE_COMMITTERS {
            active_members.push((current_members, new_group));
        }

        // If committers are full but processors are not yet full, chance to become committer. If not, become processor
        else if current_members <= MAX_ACTIVE_PROCESSERS {
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
            active_members.push((current_members, new_group));
            active_members.swap_remove(sample);
        }

        current_members += 1;

        //let elapsed = now.elapsed().as_millis();
        //println!("Members: {} - Time: {} ms", current_members, elapsed);
    }

}

criterion::criterion_group!(benches, group_benchmarks);
criterion::criterion_main!(benches);