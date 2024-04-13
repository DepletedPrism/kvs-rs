use crate::Result;

mod kvs;
mod sled;

pub use crate::engines::kvs::KvStore;
pub use crate::engines::sled::SledStore;

/// Trait that describes a key/value store engine.
pub trait KvsEngine {
    /// Set a [`String`] key to a [`String`] value.
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the [`String`] key's corresponding value.
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given [`String`] key.
    fn remove(&mut self, key: String) -> Result<()>;
}
