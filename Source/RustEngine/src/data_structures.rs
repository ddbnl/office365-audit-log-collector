use futures::channel::mpsc::{Sender, Receiver};
use std::collections::HashMap;
use reqwest::header::HeaderMap;
use serde_derive::{Deserialize};

pub type JsonList = Vec<HashMap<String, serde_json::Value>>;


/// Representation of Office API json response after sending an auth request. We need the bearer
/// token.
#[derive(Deserialize, Debug)]
pub struct AuthResult {
    pub access_token: String,
}

pub struct ContentToRetrieve {
    pub content_type: String,
    pub content_id: String,
    pub expiration: String,
    pub url: String
}

/// Messages for status channel between main threads and the blob/content retrieving threads.
/// Mainly used to keep track of which content still needs retrieving and which is finished, which
/// is necessary for knowing when to terminate.
pub enum StatusMessage {
    BeingThrottled,
    FinishedContentBlobs,  // Finished getting all content blobs for e.g. Audit.Exchange
    FoundNewContentBlob,  // Found a new blob to retrieved
    RetrievedContentBlob, // Finished retrieving a new blob
}

/// Used by thread getting content blobs
pub struct GetBlobConfig {
    pub client: reqwest::Client,
    pub headers: HeaderMap,
    pub status_tx: Sender<StatusMessage>,
    pub blobs_tx: Sender<(String, String)>,
    pub blob_error_tx: Sender<(String, String)>,
    pub content_tx: Sender<ContentToRetrieve>,
}


/// Used by thread getting content
pub struct GetContentConfig {
    pub client: reqwest::Client,
    pub headers: HeaderMap,
    pub result_tx: Sender<(String, ContentToRetrieve)>,
    pub content_error_tx: Sender<ContentToRetrieve>,
    pub status_tx: Sender<StatusMessage>
}


/// Used by message loop keeping track of progress and terminating other threads when they are
/// finished.
pub struct MessageLoopConfig {
    pub status_rx: Receiver<StatusMessage>,
    pub result_rx: Receiver<(String, ContentToRetrieve)>,
    pub blobs_tx: Sender<(String, String)>,
    pub blob_error_rx: Receiver<(String, String)>,
    pub content_tx: Sender<ContentToRetrieve>,
    pub content_error_rx: Receiver<ContentToRetrieve>,
    pub urls: Vec<(String, String)>,
    pub content_types: Vec<String>,
}
