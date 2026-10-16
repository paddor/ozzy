use super::{
    CHECKPOINT_MANIFEST_FILE, Checkpoint, CheckpointError, CheckpointLimits,
    checkpoint_manifest_digest, chunk_name,
};
use crate::async_files::Access;
use crate::checkpoint::{
    CheckpointSpec, ManifestBuilder, checkpoint_name, encode_checkpoint_manifest,
};
use ozzy_io::{Handle, OpenMode, Operation};
use std::{
    io,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) access: Access,
    pub(crate) root: PathBuf,
    pub(crate) spec: CheckpointSpec,
    pub(crate) limits: CheckpointLimits,
    pub(crate) io_bytes: usize,
    pub(crate) listing: (usize, usize),
}

/// Frozen source and bounded manifest storage. Contains no complete state image.
#[derive(Debug)]
pub(crate) struct Prepared {
    config: Config,
    manifest: ManifestBuilder,
}

impl Prepared {
    pub(crate) const fn spec(&self) -> CheckpointSpec {
        self.config.spec
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.config.root.capacity() + self.manifest.descriptor_bytes()
    }

    pub(crate) fn new(config: Config, state_bytes: u64) -> Result<Self, CheckpointError> {
        if config.io_bytes == 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        let manifest = ManifestBuilder::new(config.spec, state_bytes, config.limits)?;
        validate_listing(manifest.chunk_count(), config.listing)?;
        Ok(Self { config, manifest })
    }

    pub(crate) async fn start(self) -> Result<Box<Builder>, CheckpointError> {
        let Self { config, manifest } = self;
        let checkpoints = config.root.join("checkpoints");
        let staging = config.root.join("staging");
        let final_root = checkpoints.join(checkpoint_name(config.spec.checkpoint_id));
        let target = match config.access.open_directory(final_root.clone()).await {
            Ok(handle) => {
                config.access.done(Operation::Close { handle }).await?;
                Target::Existing(Box::new(
                    Checkpoint::open(
                        config.access.clone(),
                        final_root,
                        (config.spec.group_id, config.spec.store_id),
                        config.limits,
                        config.io_bytes,
                        config.listing,
                    )
                    .await?,
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Target::New(create_staging(&config.access, &staging, config.spec).await?)
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Box::new(Builder {
            config,
            manifest,
            target,
            file: None,
            interrupted: false,
        }))
    }
}

#[derive(Debug)]
enum Target {
    New(PathBuf),
    Existing(Box<Checkpoint>),
}

/// One open chunk and incremental hash state. Cancellation poisons this builder,
/// never the live journal; its files remain unselected until explicit installation.
#[derive(Debug)]
pub(crate) struct Builder {
    config: Config,
    manifest: ManifestBuilder,
    target: Target,
    file: Option<Handle>,
    interrupted: bool,
}

impl Builder {
    pub(crate) fn retained_bytes(&self) -> usize {
        let target = match &self.target {
            Target::New(path) => path.capacity(),
            Target::Existing(checkpoint) => {
                std::mem::size_of::<Checkpoint>()
                    + checkpoint.root.capacity()
                    + checkpoint.manifest.chunks.capacity()
                        * std::mem::size_of::<crate::CheckpointChunk>()
            }
        };
        std::mem::size_of::<Self>()
            + self.config.root.capacity()
            + self.manifest.descriptor_bytes()
            + target
    }

    pub(crate) fn remaining(&self) -> u64 {
        self.manifest.remaining()
    }

    pub(crate) const fn fragment_bytes(&self) -> usize {
        self.config.io_bytes
    }

    pub(crate) async fn push(&mut self, bytes: &[u8]) -> Result<(), CheckpointError> {
        self.healthy()?;
        if bytes.is_empty()
            || bytes.len() > self.config.io_bytes
            || bytes.len() as u64 > self.remaining()
        {
            return Err(CheckpointError::LengthMismatch);
        }
        self.interrupted = true;
        self.push_inner(bytes).await?;
        self.interrupted = false;
        Ok(())
    }

    async fn push_inner(&mut self, mut bytes: &[u8]) -> Result<(), CheckpointError> {
        while !bytes.is_empty() {
            let count = bytes.len().min(self.manifest.chunk_remaining());
            let next = &bytes[..count];
            if let Target::New(root) = &self.target {
                if self.file.is_none() {
                    self.file = Some(
                        self.config
                            .access
                            .open(
                                root.join(chunk_name(self.manifest.ordinal() as u32)),
                                OpenMode::CreateNew,
                                false,
                                false,
                            )
                            .await?,
                    );
                }
                self.config
                    .access
                    .write_all(
                        self.file.as_ref().expect("opened chunk"),
                        self.manifest.chunk_written() as u64,
                        next,
                    )
                    .await?;
            }
            if let Some(chunk) = self.manifest.append(next)? {
                match &self.target {
                    Target::New(_) => {
                        let file = self.file.as_ref().expect("written chunk");
                        self.config.access.sync(file).await?;
                        self.config
                            .access
                            .done(Operation::Close {
                                handle: file.clone(),
                            })
                            .await?;
                        self.file = None;
                    }
                    Target::Existing(checkpoint) => {
                        if checkpoint.manifest.chunks.get(chunk.ordinal as usize) != Some(&chunk) {
                            return Err(CheckpointError::ImmutableConflict);
                        }
                    }
                }
            }
            bytes = &bytes[count..];
        }
        Ok(())
    }

    fn healthy(&self) -> Result<(), CheckpointError> {
        if self.interrupted {
            return Err(
                io::Error::new(io::ErrorKind::BrokenPipe, "interrupted checkpoint build").into(),
            );
        }
        Ok(())
    }

    pub(crate) async fn finish(self: Box<Self>) -> Result<Checkpoint, CheckpointError> {
        self.healthy()?;
        let Self {
            config,
            manifest,
            target,
            ..
        } = *self;
        let manifest = manifest.finish()?;
        let bytes = encode_checkpoint_manifest(&manifest, config.limits)?;
        let digest = checkpoint_manifest_digest(&bytes, config.limits)?;
        let checkpoints = config.root.join("checkpoints");
        let staging = config.root.join("staging");
        let temporary = match target {
            Target::Existing(existing) => {
                if existing.manifest != manifest || existing.digest != digest {
                    return Err(CheckpointError::ImmutableConflict);
                }
                config.access.sync_directory(checkpoints).await?;
                config.access.sync_directory(staging).await?;
                return Ok(*existing);
            }
            Target::New(root) => root,
        };
        write_new_synced(
            &config.access,
            temporary.join(CHECKPOINT_MANIFEST_FILE),
            &bytes,
            config.io_bytes,
        )
        .await?;
        config.access.sync_directory(temporary.clone()).await?;
        let final_root = checkpoints.join(checkpoint_name(config.spec.checkpoint_id));
        config
            .access
            .done(Operation::Rename {
                source: temporary,
                destination: final_root.clone(),
            })
            .await?;
        config.access.sync_directory(checkpoints).await?;
        config.access.sync_directory(staging).await?;
        Checkpoint::open(
            config.access,
            final_root,
            (config.spec.group_id, config.spec.store_id),
            config.limits,
            config.io_bytes,
            config.listing,
        )
        .await
    }
}

fn validate_listing(count: usize, listing: (usize, usize)) -> Result<(), CheckpointError> {
    let entries = count
        .checked_add(1)
        .ok_or(CheckpointError::LengthOverflow)?;
    let names = count
        .checked_mul(chunk_name(0).len())
        .and_then(|value| value.checked_add(CHECKPOINT_MANIFEST_FILE.len()))
        .ok_or(CheckpointError::LengthOverflow)?;
    if entries > listing.0 || names > listing.1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "checkpoint exceeds directory listing limits",
        )
        .into());
    }
    Ok(())
}

async fn create_staging(
    access: &Access,
    staging: &Path,
    spec: CheckpointSpec,
) -> Result<PathBuf, CheckpointError> {
    for sequence in 0..1024 {
        let path = staging.join(format!(
            ".checkpoint-{}-{}-{sequence}.tmp",
            checkpoint_name(spec.checkpoint_id),
            spec.source_manifest_generation
        ));
        match access
            .done(Operation::CreateDirectory { path: path.clone() })
            .await
        {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(CheckpointError::StagingConflict)
}

async fn write_new_synced(
    access: &Access,
    path: PathBuf,
    bytes: &[u8],
    io_bytes: usize,
) -> Result<(), CheckpointError> {
    let file = access.open(path, OpenMode::CreateNew, false, false).await?;
    let mut offset = 0;
    for chunk in bytes.chunks(io_bytes) {
        access.write_all(&file, offset, chunk).await?;
        offset += chunk.len() as u64;
    }
    access.sync(&file).await?;
    access.done(Operation::Close { handle: file }).await?;
    Ok(())
}
