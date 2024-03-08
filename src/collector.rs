use std::thread;
use std::collections::HashMap;
use log::{warn, error};
use futures::{SinkExt};
use futures::channel::mpsc::channel;
use futures::channel::mpsc::{Sender, Receiver};
use serde_json::Value;
use crate::data_structures;
use crate::api_connection;
use crate::config::{Config, ContentTypesSubConfig};
use crate::data_structures::{Caches, CliArgs};
use crate::interface::Interface;
use crate::interfaces::file_interface::FileInterface;


/// # Rust Engine
/// A class instantiated in Python. Python will call the run_once method below, which will start
/// three background threads responsible for retrieving content. Python will then call
/// the get_result method on a loop to drain the results from the results channel until it is
/// disconnected. The three background threads are:
/// - blob_thread: find content blobs and send results to content channel
/// - content_thread: retrieve content blobs from content channel, send results to results channel
/// - message_loop_thread: keep track of progress, terminate after all content is retrieved
pub struct Collector {
    args: CliArgs,
    config: Config,
    runs: HashMap<String, Vec<(String, String)>>,
    interfaces: Vec<Box<dyn Interface>>
}

impl Collector {

    pub fn new(args: CliArgs, config: Config, runs: HashMap<String, Vec<(String, String)>>) -> Collector {

        let mut interfaces: Vec<Box<dyn Interface>> = Vec::new();
        if config.output.file.is_some() {
            interfaces.push(Box::new(FileInterface::new(config.clone())));
        }
        Collector {
            args,
            config,
            runs,
            interfaces,
        }
    }

    /// Non-blocking. Call once to start retrieving logs, which will arrive in the results_rx
    /// receiver. Call get_results iteratively to drain the results channel.
    pub fn run_once(&mut self) {
        let api = api_connection::get_api_connection(
            self.args.tenant_id.clone(), self.args.client_id.clone(),
            self.args.secret_key.clone(), self.args.publisher_id.clone());
        let (mut result_rx, mut stats_rx) = get_available_content(
            api, self.config.collect.contentTypes, self.runs.clone(), &self.config);

        let mut known_blobs = self.config.load_known_blobs();
        let mut known_logs = self.config.load_known_logs();

        let mut skipped: usize = 0;
        let mut saved: usize = 0;
        let cache_size = self.config.collect.cacheSize.unwrap_or(500000);

        let mut cache = Caches::default();

        loop {

            if let Ok(Some((a, b, c, d))) = stats_rx.try_next() {
                self.output(cache);
                print!(
"Blobs found: {}\nBlobs successful: {}\nBlobs failed: {}\nBlobs retried: {}\nLogs saved: {}\nKnown logs skipped: {}",
a, b, c, d, saved, skipped);
                break
            }

            if let Ok(Some((msg, content))) = result_rx.try_next() {
                if let Ok(logs) =
                    serde_json::from_str::<(Vec<HashMap<String, Value>>)>(&msg) {

                    known_blobs.insert(content.content_id, content.expiration);
                    for mut log in logs {
                        let log_id = log.get("Id").unwrap().to_string();
                        log.insert("OriginFeed".to_string(),
                                   Value::String(content.content_type.to_string()));
                        if known_logs.contains_key(&log_id) {
                            skipped += 1;
                            continue
                        }
                        let log_creation_time = log.get("CreationTime").unwrap()
                            .to_string();
                        known_logs.insert(log_id, log_creation_time);

                        cache.insert(log, &content.content_type);
                        saved += 1;
                        if saved % cache_size == 0 {
                            self.output(cache);
                            cache = Caches::default();
                        }
                    }
                } else {
                    warn!("Skipped log that could not be parsed: {}", content.content_id)
                }
            }
        }
        self.config.save_known_blobs(&known_blobs);
        self.config.save_known_logs(&known_logs);
    }

    fn output(&mut self, cache: Caches) {

        if self.interfaces.len() == 1 {
            self.interfaces.get_mut(0).unwrap().send_logs(cache);
        } else {
            for interface in self.interfaces.iter_mut() {
                interface.send_logs(cache.clone());
            }
        }
    }
}


/// Initialize a config object for each sub thread to run
/// - Blob thread: Collect available content blobs
/// - Content thread: Collect the blobs found by blob thread
/// - Message loop: Communicates with other threads to handle retries and terminate when finished
fn initialize_configs(
    api: api_connection::ApiConnection, content_types: ContentTypesSubConfig,
    runs: HashMap<String, Vec<(String, String)>>, config: &Config)
    -> (data_structures::GetBlobConfig,
        data_structures::GetContentConfig,
        data_structures::MessageLoopConfig, Receiver<(String, String)>,
        Receiver<data_structures::ContentToRetrieve>,
        Receiver<(String, data_structures::ContentToRetrieve)>,
        Receiver<(usize, usize, usize, usize)>) {

    let urls = api_connection::create_base_urls(
        content_types, api.tenant_id, api.publisher_id, runs);

    // Create channels to communicate with async closures
    let (status_tx, status_rx):
        (Sender<data_structures::StatusMessage>, Receiver<data_structures::StatusMessage>) =
        channel(100000);
    let (blobs_tx, blobs_rx): (Sender<(String, String)>, Receiver<(String, String)>) =
        channel(100000);
    let (blob_error_tx, blob_error_rx):
        (Sender<(String, String)>, Receiver<(String, String)>) = channel(100000);
    let (content_tx, content_rx): (Sender<data_structures::ContentToRetrieve>,
                                   Receiver<data_structures::ContentToRetrieve>) =
        channel(100000);
    let (content_error_tx, content_error_rx):
        (Sender<data_structures::ContentToRetrieve>,
         Receiver<data_structures::ContentToRetrieve>) = channel(100000000);
    let (result_tx, result_rx):
        (Sender<(String, data_structures::ContentToRetrieve)>,
         Receiver<(String, data_structures::ContentToRetrieve)>) = channel(100000000);
    let (stats_tx, stats_rx):
        (Sender<(usize, usize, usize, usize)>, Receiver<(usize, usize, usize, usize)>) = channel(100000000);

    let blob_config = data_structures::GetBlobConfig { client: reqwest::Client::new(), headers: api.headers.clone(),
        status_tx: status_tx.clone(), blobs_tx: blobs_tx.clone(),
        blob_error_tx: blob_error_tx.clone(), content_tx: content_tx.clone(),
        threads: config.collect.maxThreads.unwrap_or(50)
    };

    let content_config = data_structures::GetContentConfig {
        client: reqwest::Client::new(), headers: api.headers.clone(), result_tx: result_tx.clone(),
        content_error_tx: content_error_tx.clone(), status_tx: status_tx.clone(),
        threads: config.collect.maxThreads.unwrap_or(50)
    };

    let message_loop_config = data_structures::MessageLoopConfig {
        content_tx: content_tx.clone(), blobs_tx: blobs_tx.clone(), stats_tx: stats_tx.clone(),
        urls, content_error_rx, status_rx, blob_error_rx, content_types,
        retries: config.collect.retries.unwrap_or(3)
    };
    return (blob_config, content_config, message_loop_config, blobs_rx, content_rx, result_rx, stats_rx)
}


/// Get all the available log content for a list of content types and runs (start- and end times
/// of content to receive).
fn get_available_content(api: api_connection::ApiConnection, content_types: ContentTypesSubConfig,
                         runs: HashMap<String, Vec<(String, String)>>, config: &Config)
                         -> (Receiver<(String, data_structures::ContentToRetrieve)>,
                             Receiver<(usize, usize, usize, usize)>) {

    let (blob_config, content_config, message_loop_config,
        blobs_rx, content_rx, result_rx, stats_rx)
        = initialize_configs(api, content_types, runs, config);
    spawn_blob_collector(blob_config, content_config, message_loop_config, blobs_rx, content_rx, config);
    (result_rx, stats_rx)
}

/// Spawn threads running the actual collectors, and a message loop thread to keep track of
/// progress and terminate once finished.
fn spawn_blob_collector(
    blob_config: data_structures::GetBlobConfig, content_config: data_structures::GetContentConfig,
    message_loop_config: data_structures::MessageLoopConfig, blobs_rx: Receiver<(String, String)>,
    content_rx: Receiver<data_structures::ContentToRetrieve>, config: &Config) {

    let known_blobs= config.load_known_blobs();

    thread::spawn( move || {api_connection::get_content_blobs(blob_config, blobs_rx, known_blobs);});
    thread::spawn( move || {api_connection::get_content(content_config, content_rx);});
    thread::spawn(move || {message_loop(message_loop_config)});
}

/// Receive status updates to keep track of when all content has been retrieved. Also handle
/// retrying any failed content or dropping it after too many retries. Every time content is foudn
/// awaiting_content_blobs is incremented; every time content is retrieved or could not be
/// retrieved awaiting_content_blobs is decremented. When it reaches 0 we know we are done.
#[tokio::main]
pub async fn message_loop(mut config: data_structures::MessageLoopConfig) {

    // Send base URLS for content blob retrieval then keep track of when they've all come in
    let mut awaiting_content_types:usize = 0;
    for (content_type, base_url) in config.urls.into_iter() {
        config.blobs_tx.clone().send((content_type, base_url)).await.unwrap();
        awaiting_content_types += 1;
    }
    // Keep track of found and retrieved content blobs
    let mut awaiting_content_blobs: usize = 0;
    // Keep track of retry count for failed blobs
    let mut retry_map: HashMap<String, usize> = HashMap::new();
    // Keep stats to return to python after run finishes
    let mut stats = data_structures::RunStatistics::new();
    // Loop ends with the run itself, signalling the program is done.
    loop {

        // Receive status message indicated found content and retrieved content. When all blobs have
        // been found, and all found blobs have been retrieved, we are done.
        if let Ok(Some(msg)) = config.status_rx.try_next() {
            match msg {
                // awaiting_content_types is initially the size of content type * runs for each
                // content type. When retrieving pages if we don't get a NextPageUri response
                // header, we know we have found all possible blobs for that content type and
                // we decrement awaiting_content_types. When it hits 0 we know we found all
                // content that can possible be retrieved.
                data_structures::StatusMessage::FinishedContentBlobs => {
                    awaiting_content_types = awaiting_content_types.saturating_sub(1);
                    if awaiting_content_types == 0 && awaiting_content_blobs == 0 {
                        break
                    }
                },
                // We have found a new content blob while iterating through the pages of them.
                // It has been queued up to be retrieved.
                data_structures::StatusMessage::FoundNewContentBlob => {
                    awaiting_content_blobs +=1;
                    stats.blobs_found += 1;
                },
                // A queued up content blob has actually been retrieved so we are done with it.
                // When awaiting_content_blobs hits 0 we are done retrieving all actual content
                // and we can exit.
                data_structures::StatusMessage::RetrievedContentBlob => {
                    awaiting_content_blobs -= 1;
                    stats.blobs_successful += 1;
                    if awaiting_content_types == 0 && awaiting_content_blobs == 0 {
                        config.content_tx.close_channel();
                        break;
                    }
                },
                // A queued up content blob could not be retrieved so we are done with it.
                // When awaiting_content_blobs hits 0 we are done retrieving all actual content
                // and we can exit.
                data_structures::StatusMessage::ErrorContentBlob => {
                    awaiting_content_blobs -= 1;
                    stats.blobs_error += 1;
                    if awaiting_content_types == 0 && awaiting_content_blobs == 0 {
                        config.content_tx.close_channel();
                        break;
                    }
                }
                data_structures::StatusMessage::BeingThrottled => warn!("Throttled!"),  // TODO: handle being throttled
            }
        }
        // Check channel for content pages that could not be retrieved and retry them the user
        // defined amount of times. If we can't in that amount of times then give up.
        if let Ok(Some((content_type, url))) = config.blob_error_rx.try_next() {
            if retry_map.contains_key(&url) {
                let retries_left = retry_map.get_mut(&url).unwrap();
                if retries_left == &mut 0 {
                    error!("Gave up on blob {}", url);
                    awaiting_content_types -= 1;
                    stats.blobs_error += 1;
                } else {
                    *retries_left -= 1;
                    stats.blobs_retried += 1;
                    warn!("Retry blob {} {}", retries_left, url);
                    config.blobs_tx.send((content_type, url)).await.unwrap();

                }
            }  else {
                retry_map.insert(url.clone(), config.retries - 1);
                stats.blobs_retried += 1;
                warn!("Retry blob {} {}", config.retries - 1, url);
                config.blobs_tx.send((content_type, url)).await.unwrap();
            }
        };
        // Check channel for content blobs that could not be retrieved and retry them the user
        // defined amount of times. If we can't in that amount of times then give up.
        if let Ok(Some(content)) = config.content_error_rx.try_next() {
            if retry_map.contains_key(&content.url) {
                let retries_left = retry_map.get_mut(&content.url).unwrap();
                if retries_left == &mut 0 {
                    error!("Gave up on content {}", content.url);
                    awaiting_content_blobs -= 1;
                    stats.blobs_error += 1;
                } else {
                    *retries_left -= 1;
                    stats.blobs_retried += 1;
                    warn!("Retry content {} {}", retries_left, content.url);
                    config.content_tx.send(content).await.unwrap();

                }
            }  else {
                retry_map.insert(content.url.to_string(), config.retries - 1);
                stats.blobs_retried += 1;
                warn!("Retry content {} {}", config.retries - 1, content.url);
                config.content_tx.send(content).await.unwrap();
            }
        }
        print!("{esc}[2J{esc}[1;1H", esc = 27 as char);
        println!{"Pending content types: {}, Pending content blobs: {}",
                 awaiting_content_types, awaiting_content_blobs}
    }
    // We send back stats after exiting the loop, signalling the end of the run.
    config.stats_tx.send((stats.blobs_found, stats.blobs_successful, stats.blobs_retried,
                          stats.blobs_error)).await.unwrap();
}
