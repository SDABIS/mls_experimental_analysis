use std::collections::HashSet;

use cpu_time::ThreadTime;
use openmls_traits::{crypto::OpenMlsCrypto, random::OpenMlsRand, signatures::Signer};
use tls_codec::Serialize;
use crate::prelude::LeafNode;
use crate::treesync::node::NodeReference;
use crate::treesync::EncryptionKey;

use crate::{
    binary_tree::LeafNodeIndex,
    credentials::CredentialWithKey,
    error::LibraryError,
    extensions::Extensions,
    group::{create_commit::CommitType, errors::CreateCommitError},
    schedule::CommitSecret,
    treesync::{
        node::{
            encryption_keys::EncryptionKeyPair,
            leaf_node::{Capabilities, LeafNodeParameters, UpdateLeafNodeParams},
            parent_node::PlainUpdatePathNode,
        },
        treekem::UpdatePath,
    },
};

use super::PublicGroupDiff;

/// A helper struct which contains the values resulting from the preparation of
/// a commit with path.
#[derive(Default, Clone)]
pub struct PathComputationResult {
    pub(crate) commit_secret: Option<CommitSecret>,
    pub(crate) encrypted_path: Option<UpdatePath>,
    pub(crate) plain_path: Option<Vec<PlainUpdatePathNode>>,
    pub(crate) new_keypairs: Vec<EncryptionKeyPair>,
}

impl PublicGroupDiff<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn compute_path(
        &mut self,
        rand: &impl OpenMlsRand,
        crypto: &impl OpenMlsCrypto,
        leaf_index: LeafNodeIndex,
        exclusion_list: HashSet<&LeafNodeIndex>,
        commit_type: &CommitType,
        leaf_node_params: &LeafNodeParameters,
        signer: &impl Signer,
        gc_extensions: Option<Extensions>,
    ) -> Result<(PathComputationResult, u128, u128), CreateCommitError> {
        let ciphersuite = self.group_context().ciphersuite();

        let leaf_node_params = if let CommitType::External(credential_with_key) = commit_type {
            let capabilities = match leaf_node_params.capabilities() {
                Some(c) => c.to_owned(),
                None => Capabilities::default(),
            };

            let extensions = match leaf_node_params.extensions() {
                Some(e) => e.to_owned(),
                None => Extensions::default(),
            };

            UpdateLeafNodeParams {
                credential_with_key: credential_with_key.clone(),
                capabilities,
                extensions,
            }
        } else {
            let leaf = self
                .diff
                .leaf(leaf_index)
                .ok_or_else(|| LibraryError::custom("Couldn't find own leaf"))?;

            let credential_with_key = match leaf_node_params.credential_with_key() {
                Some(cwk) => cwk.to_owned(),
                None => CredentialWithKey {
                    credential: leaf.credential().clone(),
                    signature_key: leaf.signature_key().clone(),
                },
            };

            let capabilities = match leaf_node_params.capabilities() {
                Some(c) => c.to_owned(),
                None => leaf.capabilities().clone(),
            };

            let extensions = match leaf_node_params.extensions() {
                Some(e) => e.to_owned(),
                None => leaf.extensions().clone(),
            };

            UpdateLeafNodeParams {
                credential_with_key,
                capabilities,
                extensions,
            }
        };

        let now = ThreadTime::now();
        // For External Commits, we temporarily add a placeholder leaf node to the tree, because it
        // might be required to make the tree grow to the right size. If we
        // don't do that, calculating the direct path might fail. It's important
        // to not do anything with the value of that leaf until it has been
        // replaced.
        if let CommitType::External(_) = commit_type {
            let leaf_node = LeafNode::new_placeholder();
            self.diff.add_leaf(leaf_node)?;
        }
        let path_indices = self.diff.filtered_direct_path(leaf_index);

        let now_path = ThreadTime::now();
        // We calculate the parent hash so that we can use it for a fresh leaf
        let (path, plain_path, parent_keypairs, commit_secret) =
            self.diff.derive_path(rand, crypto, ciphersuite, path_indices)?;

        let path_time = now_path.elapsed().as_micros();
        // Derive and apply an update path based on the previously
        // generated new leaf.
        let mut new_keypairs = self.diff.apply_own_update_path(
            rand,
            crypto,
            signer,
            ciphersuite,
            commit_type,
            self.group_context().group_id().clone(),
            leaf_index,
            leaf_node_params,
            path
        )?;
        new_keypairs.extend(parent_keypairs);


        // After we've processed the path, we can update the group context s.t.
        // the updated group context is used for path secret encryption. Note
        // that we have not yet updated the confirmed transcript hash.
        self.update_group_context(gc_extensions)?;

        let serialized_group_context = self
            .group_context()
            .tls_serialize_detached()
            .map_err(LibraryError::missing_bound_check)?;

        // Copath resolutions with the corresponding public keys.
        let copath_resolutions = self.diff
            .filtered_copath_resolutions(leaf_index, &exclusion_list)
            .into_iter()
            .map(|resolution| {
                resolution
                    .into_iter()
                    .map(|(_, node_ref)| match node_ref {
                        NodeReference::Leaf(leaf) => leaf.encryption_key().clone(),
                        NodeReference::Parent(parent) => parent.encryption_key().clone(),
                    })
                    .collect::<Vec<EncryptionKey>>()
            })
            .collect::<Vec<Vec<EncryptionKey>>>();
        let tree_time = now.elapsed().as_micros() - path_time;

        
        let now = ThreadTime::now();
        // Encrypt the path to the correct recipient nodes.
        let encrypted_path = self.diff.encrypt_path(
            crypto,
            ciphersuite,
            &plain_path,
            &serialized_group_context,
            copath_resolutions,
        )?;
        let encrypt_path_time = now.elapsed().as_micros() + path_time;

        let leaf_node = self
            .diff
            .leaf(leaf_index)
            .ok_or_else(|| LibraryError::custom("Couldn't find own leaf"))?
            .clone();
        let encrypted_path = UpdatePath::new(leaf_node, encrypted_path);
        Ok((PathComputationResult {
            commit_secret: Some(commit_secret),
            encrypted_path: Some(encrypted_path),
            plain_path: Some(plain_path),
            new_keypairs,
        }, tree_time, encrypt_path_time))
    }

}
impl PathComputationResult {
    pub fn number_of_ciphertexts (&self) -> Result<usize, LibraryError> {
        match &self.encrypted_path {
            None => Err(LibraryError::custom("No encrypted path found")),
            Some(path) => {
                Ok(path.number_of_ciphertexts())
            }
        }
    }
}
