use super::{Access, IndexBuildError, Limits, RunKind};
use ozzy_io::Operation;
use std::{collections::BTreeSet, io, path::PathBuf};

#[derive(Debug)]
pub(super) struct Workspace {
    pub(super) access: Access,
    pub(super) limits: Limits,
    path: PathBuf,
    files: BTreeSet<PathBuf>,
    name_bytes: usize,
    next: u64,
}

impl Workspace {
    pub(super) async fn create(
        access: Access,
        parent: PathBuf,
        segment: u64,
        limits: Limits,
    ) -> Result<Self, IndexBuildError> {
        for sequence in 0..1024 {
            let path = parent.join(format!("index-{segment}-0-{sequence}"));
            match access
                .done(Operation::CreateDirectory { path: path.clone() })
                .await
            {
                Ok(()) => {
                    return Ok(Self {
                        access,
                        limits,
                        path,
                        files: BTreeSet::new(),
                        name_bytes: 0,
                        next: 0,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(IndexBuildError::WorkspaceExhausted)
    }

    pub(super) fn next_run(&mut self, kind: RunKind) -> Result<PathBuf, IndexBuildError> {
        let name = format!("{}-{}.run", kind.name(), self.next);
        self.next = self
            .next
            .checked_add(1)
            .ok_or(IndexBuildError::InvalidBuildLimits)?;
        self.reserve(name)
    }

    pub(super) fn reserve(&mut self, name: String) -> Result<PathBuf, IndexBuildError> {
        let names = self
            .name_bytes
            .checked_add(name.len())
            .ok_or(IndexBuildError::InvalidBuildLimits)?;
        if self.files.len() >= self.limits.directory_entries
            || names > self.limits.directory_name_bytes
        {
            return Err(IndexBuildError::RunLimitExceeded(
                self.limits.directory_entries,
            ));
        }
        let path = self.path.join(name);
        if !self.files.insert(path.clone()) {
            return Err(IndexBuildError::InvalidRun);
        }
        self.name_bytes = names;
        Ok(path)
    }

    pub(super) async fn remove(&mut self, path: &PathBuf) -> Result<(), IndexBuildError> {
        if !self.files.contains(path) {
            return Err(IndexBuildError::InvalidRun);
        }
        self.access
            .done(Operation::RemoveFile { path: path.clone() })
            .await?;
        self.files.remove(path);
        self.name_bytes -= path.file_name().expect("owned run name").len();
        Ok(())
    }

    pub(super) async fn cleanup(mut self) -> Result<(), IndexBuildError> {
        while let Some(path) = self.files.first().cloned() {
            self.remove(&path).await?;
        }
        self.access
            .done(Operation::RemoveDirectory { path: self.path })
            .await?;
        Ok(())
    }
}
