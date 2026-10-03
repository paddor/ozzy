//! Metadata validation only. Segment/checkpoint history must still be validated
//! before these positions can grant journal or replication authority.

use super::Directory;
use crate::directory::publication::algorithm::Io;
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
        let identity = decode_group_identity(
            &self
                .files
                .read_exact("identity", GROUP_IDENTITY_BYTES)
                .await?,
        )?;
        if identity != expected {
            return Err(DirectoryError::IdentityMismatch);
        }
        let current = decode_current(&self.files.read_exact("CURRENT", CURRENT_BYTES).await?)?;
        if current.group_id != identity.group_id || current.store_id != identity.store_id {
            return Err(DirectoryError::CurrentMismatch);
        }
        let bytes = self
            .files
            .read(
                &format!("MANIFEST.{}", current.generation),
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
        let protected = if manifest.durable_evidence {
            evidence::Copies::new(&self.files.evidence_records().await?, &manifest)?
                .protected(&manifest)?
        } else {
            manifest.accepted
        };
        let configuration = match self
            .files
            .read("CONFIGURATION", None, GROUP_CONFIGURATION_MAX_BYTES)
            .await
        {
            Ok(bytes) if bytes.is_empty() => return Err(DirectoryError::ConfigurationLength),
            Ok(bytes) => Some(bytes),
            Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        if let Some(expected) = expected_configuration {
            if configuration.as_deref() != Some(expected) {
                return Err(DirectoryError::ConfigurationMismatch);
            }
            self.files.sync_file("CONFIGURATION").await?;
            self.files.sync_directory().await?;
        }
        Ok(Selected {
            current,
            manifest,
            configuration,
            protected,
        })
    }
}
