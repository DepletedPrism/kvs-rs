use crate::{Error, KvsEngine, Result};
use std::ops::Deref;

/// implement `KvsEngine` for `sled` for benchmarking
pub struct SledStore(sled::Db);

impl SledStore {
    /// Open a Db with a default configuration at the specified directory.
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<SledStore> {
        Ok(SledStore(sled::open(path.into())?))
    }
}

impl Deref for SledStore {
    type Target = sled::Tree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl KvsEngine for SledStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.insert(key, value.into_bytes())?;
        self.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(sled::Tree::get(self, key)?
            .map(|ivec| ivec.as_ref().to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        sled::Tree::remove(self, key)?.ok_or(Error::NonexistentKey)?;
        self.flush()?;
        Ok(())
    }
}
