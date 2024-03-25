use std::{error, fmt, io, num, string};

/// Error type for `kvs`
#[derive(Debug)]
pub enum Error {
    /// std::io::Error
    Io(io::Error),
    /// serde_json::Error
    Json(serde_json::Error),
    /// std::string::from_utf8
    Utf8(string::FromUtf8Error),
    /// std::num::ParseIntError
    ParseInt(num::ParseIntError),
    /// No such a key
    NonexistentKey(String),
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::Json(value)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(value: string::FromUtf8Error) -> Self {
        Error::Utf8(value)
    }
}

impl From<num::ParseIntError> for Error {
    fn from(value: num::ParseIntError) -> Self {
        Error::ParseInt(value)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{}", e),
            Self::Json(e) => write!(f, "{}", e),
            Self::Utf8(e) => write!(f, "{}", e),
            Self::ParseInt(e) => write!(f, "{}", e),
            Self::NonexistentKey(key) => write!(f, "No such a key named as `{}`", key),
        }
    }
}

impl error::Error for Error {
    // benefit from default implementations
}
