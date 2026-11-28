use ds_lib::WelcomeAcknowledgement;
use openmls::prelude::tls_codec::{Serialize, Size};

#[test]
fn welcome_acknowledgement_size_matches_encoded_length() {
    let acknowledgement = WelcomeAcknowledgement {
        sender: "Bob".into(),
        group: "study".into(),
    };

    let mut encoded = Vec::new();
    acknowledgement.tls_serialize(&mut encoded).unwrap();
    assert_eq!(acknowledgement.tls_serialized_len(), encoded.len());
}
