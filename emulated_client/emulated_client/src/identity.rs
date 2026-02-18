use std::collections::HashMap;

use openmls::prelude::{*};
use openmls_basic_credential::SignatureKeyPair;
use openmls_traits::OpenMlsProvider;

#[derive(Debug)]
pub struct Identity {
    pub(crate) kp: HashMap<Vec<u8>, KeyPackage>,
    pub(crate) credential_with_key: CredentialWithKey,
    pub(crate) signer: SignatureKeyPair,
}

impl Identity {
    pub fn new(
        ciphersuite: Ciphersuite,
        crypto: &impl OpenMlsProvider,
        id: &[u8],
    ) -> Self {
        let credential = Credential::new(CredentialType::Basic, id.to_vec());
        let signature_keys = SignatureKeyPair::new(ciphersuite.signature_algorithm()).unwrap();
        let credential_with_key = CredentialWithKey {
            credential,
            signature_key: signature_keys.to_public_vec().into(),
        };
        signature_keys.store(crypto.storage()).unwrap();

        let key_package = KeyPackage::builder()
            .leaf_node_capabilities(
                Capabilities::default()
            )
            .key_package_extensions(
                Extensions::single(Extension::LastResort(LastResortExtension::default()))
            )
            .build(
                ciphersuite,
                crypto,
                &signature_keys,
                credential_with_key.clone(),
            )
            .unwrap().key_package().clone();

        Self {
            kp: HashMap::from([(
                key_package
                    .hash_ref(crypto.crypto())
                    .unwrap()
                    .as_slice()
                    .to_vec(),
                key_package,
            )]),
            credential_with_key,
            signer: signature_keys,
        }
    }

    /// Create an additional key package using the credential_with_key/signer bound to this identity
    pub fn add_key_package(
        &mut self,
        ciphersuite: Ciphersuite,
        crypto: &impl OpenMlsProvider,
    ) -> KeyPackage {
        let key_package = KeyPackage::builder()
            .leaf_node_capabilities(
                Capabilities::default()
            )
            .key_package_extensions(
                Extensions::single(Extension::LastResort(LastResortExtension::default()))
            )
            .build(
                ciphersuite,
                crypto,
                &self.signer,
                self.credential_with_key.clone(),
            )
            .unwrap().key_package().clone();

        self.kp.insert(
            key_package
                .hash_ref(crypto.crypto())
                .unwrap()
                .as_slice()
                .to_vec(),
            key_package.clone(),
        );
        key_package
    }

    /// Get the plain identity as byte vector.
    pub fn identity(&self) -> &[u8] {
        self.credential_with_key.credential.identity()
    }

    pub fn signer(&self) -> &SignatureKeyPair {
        &self.signer
    }

    pub fn key_packages(&self) -> Vec<KeyPackage> {
        self.kp.values().cloned().collect()
    }

    pub fn credential_with_key(&self) -> &CredentialWithKey {
        &self.credential_with_key
    }
}
