use futures::channel::mpsc::{Sender, Receiver};
use std::collections::HashMap;
use reqwest::header::HeaderMap;
use serde_derive::Deserialize;
use clap::Parser;
use log::warn;
use serde_json::Value;
use crate::config::ContentTypesSubConfig;

/// List of JSON responses (used to represent content blobs)
pub type ArbitraryJson = HashMap<String, Value>;
pub type JsonList = Vec<ArbitraryJson>;


#[derive(Default, Clone, Debug)]
pub struct Caches {
    pub general: JsonList,
    pub aad: JsonList,
    pub exchange: JsonList,
    pub sharepoint: JsonList,
    pub dlp: JsonList,
    pub size: usize,
}
impl Caches {

    pub fn full(&self) -> bool {
        let size = self.general.len()
            + self.aad.len()
            + self.exchange.len()
            + self.sharepoint.len()
            + self.dlp.len();
        size >= self.size
    }

    pub fn new(size: usize) -> Self {
        let mut cache = Caches::default();
        cache.size = size;
        cache
    }
    pub fn insert(&mut self, log: ArbitraryJson, content_type: &String) {
        match content_type.as_str() {
            "Audit.General" => self.general.push(log),
            "Audit.AzureActiveDirectory" => self.aad.push(log),
            "Audit.Exchange" => self.exchange.push(log),
            "Audit.SharePoint" => self.sharepoint.push(log),
            "DLP.All" => self.dlp.push(log),
            _ => warn!("Unknown content type cached: {}", content_type),
        }
    }

    pub fn get_all_types(&self) -> [(String, &JsonList); 5] {
        [
            ("Audit.General".to_string(), &self.general),
            ("Audit.AzureActiveDirectory".to_string(), &self.aad),
            ("Audit.Exchange".to_string(), &self.exchange),
            ("Audit.SharePoint".to_string(), &self.sharepoint),
            ("DLP.All".to_string(), &self.dlp)
        ]
    }

    pub fn get_all(&mut self) -> [&mut JsonList; 5] {
        [
            &mut self.general,
            &mut self.aad,
            &mut self.exchange,
            &mut self.sharepoint,
            &mut self.dlp
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
#[derive(Debug, Clone)]
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
    FinishedContentBlobs,  // Finished getting all content blobs for e.g. Audit.Exchange
    FoundNewContentBlob,  // Found a new blob to retrieved
    RetrievedContentBlob, // Finished retrieving a new blob
    ErrorContentBlob, // Could not retrieve a blob
    BeingThrottled,
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
    pub duplicate: usize
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
    pub kill_rx: tokio::sync::mpsc::Receiver<bool>,
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
#[derive(Default, Copy, Clone, Debug)]
pub struct RunStatistics {
    pub blobs_found: usize,
    pub blobs_successful: usize,
    pub blobs_error: usize,
    pub blobs_retried: usize,
}


#[derive(Default, Clone)]
pub struct RunState {
    pub awaiting_content_types: usize,
    pub awaiting_content_blobs: usize,
    pub stats: RunStatistics,
    pub rate_limited: bool,
}

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
/// Collect audit logs from Office Management APIs.
/// Complete all preparation steps in README.MD
/// to prepare your tenant for collection. Then prepare your config file to specify outputs and
/// collection options (check the examples folder in the repo). Then run the tool with below options.
pub struct CliArgs {

    #[arg(long, help = "ID of tenant to retrieve logs for.")]
    pub tenant_id: String,

    #[arg(long, help = "Client ID of app registration used to retrieve logs.")]
    pub client_id: String,

    #[arg(long, help = "Secret key of app registration used to retrieve logs")]
    pub secret_key: String,

    #[arg(short, long, default_value = "12345678-1234-1234-1234-123456789123", help = "Publisher ID, set to tenant-id if left empty.")]
    pub publisher_id: String,

    #[arg(long, help = "Path to mandatory config file.")]
    pub config: String,

    #[arg(short, long, default_value = "", help = "Shared key for Azure Log Analytics Workspace.")]
    pub oms_key: String,

    #[arg(short, long, required = false, help = "Interactive interface for (load) testing.")]
    pub interactive: bool,
}
