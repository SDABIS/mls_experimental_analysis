use openmls::prelude::{tls_codec::Deserialize, *};
use openmls_basic_credential::SignatureKeyPair;
use openmls_rust_crypto::OpenMlsRustCrypto;

const SUITE: Ciphersuite = Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;

fn identity(name: &str) -> (CredentialWithKey, SignatureKeyPair) {
    let signer = SignatureKeyPair::new(SUITE.signature_algorithm()).unwrap();
    let credential = CredentialWithKey {
        credential: BasicCredential::new(name.as_bytes().to_vec()).into(),
        signature_key: signer.to_public_vec().into(),
    };
    (credential, signer)
}

fn key_package(provider: &OpenMlsRustCrypto, name: &str) -> KeyPackage {
    let (credential, signer) = identity(name);
    KeyPackage::builder()
        .build(SUITE, provider, &signer, credential)
        .unwrap()
        .key_package()
        .clone()
}

fn check_join_after_removals(initial_members: u32, removed_slots: &[u32], expected_slot: u32) {
    // Fill consecutive slots so the test controls which gaps the removals leave behind.
    let creator = OpenMlsRustCrypto::default();
    let (credential, signer) = identity("creator");
    let config = MlsGroupCreateConfig::builder().ciphersuite(SUITE).build();
    let mut group = MlsGroup::new(&creator, &signer, &config, credential).unwrap();
    let packages: Vec<_> = (1..initial_members)
        .map(|i| key_package(&creator, &format!("member-{i}")))
        .collect();
    group.add_members(&creator, &signer, &packages).unwrap();
    group.merge_pending_commit(&creator).unwrap();

    // Apply each removal in its own epoch, as a group would during ordinary use.
    for &slot in removed_slots {
        group
            .remove_members(&creator, &signer, &[LeafNodeIndex::new(slot)])
            .unwrap();
        group.merge_pending_commit(&creator).unwrap();
    }

    // Let a fresh member verify the Welcome against the separately exported tree.
    let joiner = OpenMlsRustCrypto::default();
    let package = key_package(&joiner, "new member");
    let (_, message, _) = group.add_members(&creator, &signer, &[package]).unwrap();
    group.merge_pending_commit(&creator).unwrap();
    let message = MlsMessageIn::tls_deserialize_exact(message.to_bytes().unwrap()).unwrap();
    let MlsMessageBodyIn::Welcome(welcome) = message.extract() else {
        panic!("expected a Welcome after adding a member");
    };
    let joined = StagedWelcome::new_from_welcome(
        &joiner,
        config.join_config(),
        welcome,
        Some(group.export_ratchet_tree().into()),
    )
    .expect("the new member should accept the exported tree")
    .into_group(&joiner)
    .unwrap();

    assert_eq!(joined.own_leaf_index().u32(), expected_slot);
    assert_eq!(
        joined.epoch_authenticator().as_slice(),
        group.epoch_authenticator().as_slice()
    );
}

#[test]
fn welcome_after_removing_the_right_half_of_the_tree() {
    // Slots 0, 1, and 2 survive. The joiner fits in slot 3 of a four-slot tree.
    check_join_after_removals(5, &[4, 3], 3);
}

#[test]
fn welcome_after_shrinking_to_one_member() {
    // Only the creator remains. The next join grows the tree from one slot to two.
    check_join_after_removals(4, &[3, 2, 1], 1);
}

#[test]
fn welcome_reuses_an_interior_gap_with_the_right_edge_occupied() {
    // Slot 3 remains occupied, so the next join should fill the gap at slot 2.
    check_join_after_removals(4, &[2], 2);
}

#[test]
fn welcome_reuses_slot_one_after_removing_from_a_four_member_tree() {
    // This gap exercises leftmost() on a subtree that begins at leaf zero.
    check_join_after_removals(4, &[1], 1);
}
