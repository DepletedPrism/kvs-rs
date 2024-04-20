//! `kvs` is a simple key-value store engine written in Rust.

/// Protocol used for communicating between client and server.
pub mod common;
mod engines;
mod error;
/// Thread pool implementations
pub mod thread_pool;

// re-export names with pub use
pub use crate::engines::{KvStore, KvsEngine, SledStore};
pub use crate::error::Error;

/// to simplify concrete implementations
pub type Result<T> = std::result::Result<T, Error>;
