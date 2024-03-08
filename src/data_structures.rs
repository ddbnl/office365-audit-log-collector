use futures::channel::mpsc::{Sender, Receiver};
use std::collections::HashMap;
use reqwest::header::HeaderMap;
use serde_derive::{Deserialize};
use clap::Parser;
use log::warn;
use serde_json::Value;
use crate::config::ContentTypesSubConfig;

/// List of JSON responses (used to represent content blobs)
pub type JsonList = Vec<HashMap<String, serde_json::Value>>;


#[derive(Default, Clone)]
pub struct Caches {
    general: Vec<HashMap<String, Value>>,
    aad: Vec<HashMap<String, Value>>,
    exchange: Vec<HashMap<String, Value>>,
    sharepoint: Vec<HashMap<String, Value>>,
    dlp: Vec<HashMap<String, Value>>,
}
impl Caches {
    pub fn insert(&mut self, log: HashMap<String, Value>, content_type: &String) {
        match content_type.as_str() {
            "Audit.General" => self.general.push(log),
            "Audit.AzureActiveDirectory" => self.aad.push(log),
            "Audit.Exchange" => self.exchange.push(log),
            "Audit.SharePoint" => self.sharepoint.push(log),
            "DLP.All" => self.dlp.push(log),
            _ => warn!("Unknown content type cached: {}", content_type),
        }
    }

    pub fn get_all_types(&self) -> [(String, &Vec<HashMap<String, Value>>); 5] {
        [
            ("Audit.General".to_string(), &self.general),
            ("Audit.AzureActiveDirectory".to_string(), &self.aad),
            ("Audit.Exchange".to_string(), &self.exchange),
            ("Audit.SharePoint".to_string(), &self.sharepoint),
            ("DLP.All".to_string(), &self.dlp)
        ]
    }

    pub fn get_all(&self) -> [&Vec<HashMap<String, Value>>; 5] {
        [
            &self.general,
            &self.aad,
            &self.exchange,
            &self.sharepoint,
            &self.dlp
        ]
    }
}


/// Representation of Office API json response after sending an auth request. We need the bearer
/// token.
#[derive(Deserialize, Debug)]
pub struct AuthResult {
    pub access_token: String,
}


/// Representation of content we need to retrieve. ID, expiration and content type are passed to
/// python along with the retrieved content. ID an expiration are needed for avoiding known logs,
/// content type for categorization in outputs.
#[derive(Debug)]
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
    ErrorContentBlob, // Could not retrieve a blob
}

/// Used by thread getting content blobs
pub struct GetBlobConfig {
    pub client: reqwest::Client,
    pub headers: HeaderMap,
    pub status_tx: Sender<StatusMessage>,
    pub blobs_tx: Sender<(String, String)>,
    pub blob_error_tx: Sender<(String, String)>,
    pub content_tx: Sender<ContentToRetrieve>,
    pub threads: usize,
}


/// Used by thread getting content
pub struct GetContentConfig {
    pub client: reqwest::Client,
    pub headers: HeaderMap,
    pub result_tx: Sender<(String, ContentToRetrieve)>,
    pub content_error_tx: Sender<ContentToRetrieve>,
    pub status_tx: Sender<StatusMessage>,
    pub threads: usize,
}


/// Used by message loop keeping track of progress and terminating other threads when they are
/// finished.
pub struct MessageLoopConfig {
    pub status_rx: Receiver<StatusMessage>,
    pub stats_tx: Sender<(usize, usize, usize, usize)>,
    pub blobs_tx: Sender<(String, String)>,
    pub blob_error_rx: Receiver<(String, String)>,
    pub content_tx: Sender<ContentToRetrieve>,
    pub content_error_rx: Receiver<ContentToRetrieve>,
    pub urls: Vec<(String, String)>,
    pub content_types: ContentTypesSubConfig,
    pub retries: usize,
}


/// These stats to show to end-user.
pub struct RunStatistics {
    pub blobs_found: usize,
    pub blobs_successful: usize,
    pub blobs_error: usize,
    pub blobs_retried: usize,
}
impl RunStatistics {
    pub fn new() -> RunStatistics {
        RunStatistics {
            blobs_found: 0,
            blobs_successful: 0,
            blobs_error: 0,
            blobs_retried: 0
        }
    }
}


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
/// Collect audit logs from Office Management APIs.
/// Complete all preparation steps in README.MD
/// to prepare your tenant for collection. Then prepare your config file to specify outputs and
/// collection options (check the examples folder in the repo). Then run the tool with below options.
pub struct CliArgs {

    #[arg(long)]
    pub tenant_id: String,

    #[arg(long)]
    pub client_id: String,

    #[arg(long)]
    pub secret_key: String,

    #[arg(short, long, default_value = "12345678-1234-1234-1234-123456789123")]
    pub publisher_id: String,

    #[arg(long)]
    pub config: String,

    #[arg(short, long, default_value = "")]
    pub table_string: String,

    #[arg(short, long, default_value = "")]
    pub blob_string: String,

    #[arg(short, long, default_value = "")]
    pub sql_string: String,

    #[arg(short, long, required = false)]
    pub interactive_subscriber: bool,
}
