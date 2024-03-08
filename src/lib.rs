#![deny(missing_docs)]
//! `kvs` is a simple key-value store engine written in Rust.

use std::collections::BTreeMap;

/// Used for store key-value pairs in memory.
///
/// # Examples
///
/// ```rust
/// use kvs::KvStore;
///
/// let mut map = KvStore::new();
/// map.set("114".to_owned(), "514".to_owned());
///
/// assert_eq!(map.get("114".to_owned()), Some("514".to_owned()));
/// ```
pub struct KvStore {
    mp: BTreeMap<String, String>,
}

impl KvStore {
    /// Create a new KvStore which store key-value pairs.
    pub fn new() -> KvStore {
        KvStore {
            mp: BTreeMap::new(),
        }
    }

    /// Set a [`String`] key and a [`String`] value.
    pub fn set(&mut self, key: String, value: String) {
        self.mp.insert(key, value);
    }

    /// Get a [`String`] key's value.
    pub fn get(&mut self, key: String) -> Option<String> {
        self.mp.get(&key).cloned()
    }

    /// Remove a [`String`] key.
    pub fn remove(&mut self, key: String) {
        self.mp.remove(&key);
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}
