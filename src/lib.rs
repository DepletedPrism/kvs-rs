#![deny(missing_docs)]
//! `kvs` is a simple key-value store engine written in Rust.

mod error;
mod io;
mod store;

// re-export names with pub use
pub use crate::error::Error;
pub use crate::store::KvStore;

/// to simplify concrete implementations
pub type Result<T> = std::result::Result<T, Error>;
