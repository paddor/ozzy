//! Opening a segment writer: create a new file, recover an existing one, or
//! initialize a prepared successor.

use super::super::{RecoveryValidation, SegmentState, recovery};
use super::{File, Opened, Options, Start, Writer};
use crate::{
    CanonicalRecoveryRequirements, CodecError, DecodeLimits, SegmentHeader, SegmentWriteMode,
    WriterError, encode_segment_header,
};
use ozzy_io::{OpenMode, Operation};

impl Writer {
    /// A freshly allocated, exclusively owned successor. Only the directory's
    /// private prepared-file type may supply this handle; never adopt leftovers.
    pub(crate) async fn initialize_prepared(
        opened: Opened,
        header: SegmentHeader,
        start: Start,
        options: Options,
    ) -> Result<Self, WriterError> {
        let Opened {
            access, buffered, ..
        } = &opened;
        options.validate(&access.io)?;
        if start.first_group_number == 0 {
            return Err(CodecError::InvalidGroupNumber.into());
        }
        if header.capacity() > options.max_segment_bytes
            || access.length(buffered).await? > header.capacity()
        {
            return Err(WriterError::InvalidSyncPosition);
        }
        access
            .write_all(buffered, 0, &encode_segment_header(&header))
            .await?;
        access.sync(buffered).await?;
        let state = SegmentState::empty(
            header,
            start.generation,
            start.first_group_number,
            start.initial_chain,
        );
        Self::finish_open(opened, state, options).await
    }

    /// Create a new preallocated segment and synchronize its header. The
    /// directory owner must still publish and synchronize its directory entry.
    /// Optional protection retains the caller's group lock through every job.
    pub async fn create(
        file: File,
        header: SegmentHeader,
        start: Start,
        options: Options,
    ) -> Result<Self, WriterError> {
        options.validate(&file.io)?;
        if start.first_group_number == 0 {
            return Err(CodecError::InvalidGroupNumber.into());
        }
        if header.capacity() > options.max_segment_bytes {
            return Err(WriterError::RecoveryLimit {
                actual: header.capacity(),
                limit: options.max_segment_bytes,
            });
        }
        let (path, access) = file.into_access();
        let buffered = access
            .open(path.clone(), OpenMode::CreateNew, false, false)
            .await?;
        access
            .done(Operation::Allocate {
                handle: buffered.clone(),
                offset: 0,
                length: header.capacity(),
            })
            .await?;
        access
            .write_all(&buffered, 0, &encode_segment_header(&header))
            .await?;
        access.sync(&buffered).await?;
        let state = SegmentState::empty(
            header,
            start.generation,
            start.first_group_number,
            start.initial_chain,
        );
        let opened = Opened {
            path,
            access,
            buffered,
        };
        Self::finish_open(opened, state, options).await
    }

    /// Validate the existing bytes before changing a damaged tail. Canonical
    /// requirements control protected-prefix checks and permitted tail repair;
    /// `None` retains the low-level writer's strict structural recovery mode.
    pub async fn recover(
        file: File,
        start: Start,
        options: Options,
        limits: DecodeLimits,
        requirements: Option<CanonicalRecoveryRequirements>,
    ) -> Result<Self, WriterError> {
        options.validate(&file.io)?;
        let (path, access) = file.into_access();
        let buffered = access
            .open(path.clone(), OpenMode::ReadWrite, false, false)
            .await?;
        let image = access.read_image(&buffered, options).await?;
        let validation = match requirements {
            Some(required) => RecoveryValidation {
                protected: required.protected,
                operation_limits: Some(required.operation_limits),
                configuration_epoch: required.configuration_epoch,
                promised_view: required.promised_view,
                discard_damaged_tail: required.discard_damaged_tail,
            },
            None => RecoveryValidation {
                protected: [None; 2],
                operation_limits: None,
                configuration_epoch: None,
                promised_view: None,
                discard_damaged_tail: false,
            },
        };
        let recovered = recovery::prepare_async(&image, start, limits, validation).await?;
        if recovered.writer.header().capacity() > options.max_segment_bytes {
            return Err(WriterError::RecoveryLimit {
                actual: recovered.writer.header().capacity(),
                limit: options.max_segment_bytes,
            });
        }
        drop(image);
        if let Some(length) = recovered.truncate {
            access
                .done(Operation::SetLength {
                    handle: buffered.clone(),
                    length,
                })
                .await?;
        }
        for range in recovered.invalidate {
            access.zero(&buffered, range, options.chunk_bytes).await?;
        }
        access.sync(&buffered).await?;
        let opened = Opened {
            path,
            access,
            buffered,
        };
        Self::finish_open(opened, recovered.writer, options).await
    }

    async fn finish_open(
        opened: Opened,
        mut state: SegmentState,
        options: Options,
    ) -> Result<Self, WriterError> {
        let Opened {
            path,
            access,
            buffered,
        } = opened;
        let data_sync = options.write_mode == SegmentWriteMode::DataSync;
        let output = if options.direct || data_sync {
            access
                .open(path, OpenMode::ReadWrite, options.direct, data_sync)
                .await?
        } else {
            buffered.clone()
        };
        state.data_sync = data_sync;
        Ok(Self {
            state,
            access,
            buffered,
            output,
            options,
            interrupted: false,
        })
    }
}
