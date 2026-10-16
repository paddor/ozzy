//! Metadata validation only. Segment/checkpoint history must still be validated
//! before these positions can grant journal or replication authority.

use super::Directory;
use crate::directory::publication::Io;
use crate::directory::{CONFIGURATION_FILE, CURRENT_FILE, IDENTITY_FILE, manifest_name};
use crate::{
    CURRENT_BYTES, CurrentReference, DirectoryError, GROUP_CONFIGURATION_MAX_BYTES,
    GROUP_IDENTITY_BYTES, GroupIdentity, LogPosition, Manifest, MetadataLimits, WriterError,
    decode_current, decode_group_identity, decode_manifest, directory::evidence, manifest_digest,
};
use std::io;

/// Exactly the metadata selected by CURRENT, with identity/digests checked.
/// This is not a recovered journal or proof that its referenced history exists.
#[derive(Debug)]
pub(crate) struct Selected {
    pub(crate) current: CurrentReference,
    pub(crate) manifest: Manifest,
    pub(crate) configuration: Option<Vec<u8>>,
    pub(crate) protected: LogPosition,
}

impl Directory {
    /// Load the selected generation, never fall back to an older manifest or
    /// manufacture absent configuration. Expected configuration is resynchronized
    /// before success, matching the existing intact-store admission boundary.
    pub(crate) async fn read_selected(
        &mut self,
        expected: GroupIdentity,
        limits: MetadataLimits,
        expected_configuration: Option<&[u8]>,
    ) -> Result<Selected, DirectoryError> {
        if self.is_faulted() {
            return Err(WriterError::Faulted.into());
        }
        expected.validate()?;
        if expected_configuration
            .is_some_and(|bytes| bytes.is_empty() || bytes.len() > GROUP_CONFIGURATION_MAX_BYTES)
        {
            return Err(DirectoryError::ConfigurationLength);
        }
        let (current, manifest) = self.read_current(expected, limits).await?;
        let protected = if manifest.durable_evidence {
            evidence::Copies::new(&self.files.evidence_records().await?, &manifest)?
                .protected(&manifest)?
        } else {
            manifest.accepted
        };
        let configuration = self.read_configuration(expected_configuration).await?;
        Ok(Selected {
            current,
            manifest,
            configuration,
            protected,
        })
    }

    /// The identity, CURRENT, and the manifest it names, each bound to the
    /// expected identity and to each other by digest and generation.
    async fn read_current(
        &mut self,
        expected: GroupIdentity,
        limits: MetadataLimits,
    ) -> Result<(CurrentReference, Manifest), DirectoryError> {
        let identity = decode_group_identity(
            &self
                .files
                .read_exact(IDENTITY_FILE, GROUP_IDENTITY_BYTES)
                .await?,
        )?;
        if identity != expected {
            return Err(DirectoryError::IdentityMismatch);
        }
        let current = decode_current(&self.files.read_exact(CURRENT_FILE, CURRENT_BYTES).await?)?;
        if current.group_id != identity.group_id || current.store_id != identity.store_id {
            return Err(DirectoryError::CurrentMismatch);
        }
        let bytes = self
            .files
            .read(
                &manifest_name(current.generation),
                None,
                limits.max_manifest_bytes,
            )
            .await?;
        if manifest_digest(&bytes, limits)? != current.manifest_digest {
            return Err(DirectoryError::CurrentMismatch);
        }
        let manifest = decode_manifest(&bytes, limits)?;
        if manifest.generation != current.generation || manifest.identity != identity {
            return Err(DirectoryError::CurrentMismatch);
        }
        Ok((current, manifest))
    }

    /// The stored configuration, if any. An expected configuration must match
    /// it exactly and is synchronized before success.
    async fn read_configuration(
        &mut self,
        expected: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, DirectoryError> {
        let configuration = match self
            .files
            .read(CONFIGURATION_FILE, None, GROUP_CONFIGURATION_MAX_BYTES)
            .await
        {
            Ok(bytes) if bytes.is_empty() => return Err(DirectoryError::ConfigurationLength),
            Ok(bytes) => Some(bytes),
            Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        if let Some(expected) = expected {
            if configuration.as_deref() != Some(expected) {
                return Err(DirectoryError::ConfigurationMismatch);
            }
            self.files.sync_file(CONFIGURATION_FILE).await?;
            self.files.sync_directory().await?;
        }
        Ok(configuration)
    }
}
