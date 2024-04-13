use serde::{Deserialize, Serialize};

/// Request from client.
#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

/// Response from server.
#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    Status(String),
}
