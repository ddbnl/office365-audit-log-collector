use std::thread;
use std::collections::HashMap;
use std::mem::swap;
use std::ops::Div;
use std::time::Instant;
use log::{warn, error, info};
use futures::{SinkExt};
use futures::channel::mpsc::channel;
use futures::channel::mpsc::{Sender, Receiver};
use serde_json::Value;
use crate::data_structures;
use crate::api_connection;
use crate::api_connection::ApiConnection;
use crate::config::{Config, ContentTypesSubConfig};
use crate::data_structures::{ArbitraryJson, Caches, CliArgs, ContentToRetrieve, JsonList};
use crate::interfaces::azure_oms_interface::OmsInterface;
use crate::interfaces::interface::Interface;
use crate::interfaces::file_interface::FileInterface;
use crate::interfaces::fluentd_interface::FluentdInterface;
use crate::interfaces::graylog_interface::GraylogInterface;


/// # Office Audit Log Collector
/// Will start three background threads responsible for retrieving content:
/// - blob_thread: find content blobs and send results to content channel
/// - content_thread: retrieve content blobs from content channel, send results to results channel
/// - message_loop_thread: keep track of progress, terminate after all content is retrieved
/// Found blobs (which contain logs) are sent to the main thread, which will check filters and known
/// logs to determine whether it must be saved. If it must be saved, it is forwarded to active
/// interfaces. Active interfaces are determined by the config file passed in by the user.
pub struct Collector {
    config: Config,
    interfaces: Vec<Box<dyn Interface>>,

    result_rx: Receiver<(String, ContentToRetrieve)>,
    stats_rx: Receiver<(usize, usize, usize, usize)>,
    kill_tx: tokio::sync::mpsc::Sender<bool>,
    known_blobs: HashMap<String, String>,
    saved: usize,
    cache: Caches,
    filters: HashMap<String, ArbitraryJson>,
}

impl Collector {

    pub fn new(args: CliArgs, config: Config, runs: HashMap<String, Vec<(String, String)>>) -> Collector {

        // Initialize interfaces
        let mut interfaces: Vec<Box<dyn Interface>> = Vec::new();
        if config.output.file.is_some() {
            interfaces.push(Box::new(FileInterface::new(config.clone())));
        }
        if config.output.fluentd.is_some() {
            interfaces.push(Box::new(FluentdInterface::new(config.clone())));
        }
        if config.output.graylog.is_some() {
            interfaces.push(Box::new(GraylogInterface::new(config.clone())));
        }
        if config.output.oms.is_some() {
            interfaces.push(Box::new(OmsInterface::new(config.clone(), args.oms_key.clone())));
        }

        // Initialize collector threads
        let api = api_connection::get_api_connection(
            args.clone(), config.clone()
        );
        api.subscribe_to_feeds();

        let known_blobs = config.load_known_blobs();
        let (result_rx, stats_rx, kill_tx) =
            get_available_content(api,
                                  config.collect.content_types,
                                  runs.clone(),
                                  &config,
                                  known_blobs.clone());

        // Initialize collector
        let cache_size = config.collect.cache_size.unwrap_or(500000);
        let cache = Caches::new(cache_size);
        let filters =
                if let Some(filter_config) = &config.collect.filter {
            filter_config.get_filters()
        } else {
            HashMap::new()
        };
        Collector {
            config,
            interfaces,
            result_rx,
            stats_rx,
            known_blobs,
            saved: 0,
            kill_tx,
            filters,
            cache
        }
    }

    /// Monitor all started content retrieval threads, processing results and terminating
    /// when all content has been retrieved (signalled by a final run stats message).
    pub async fn monitor(&mut self) {

        let start = Instant::now();
        loop {
            if let Some(timeout) = self.config.collect.global_timeout {
                if timeout > 0 && start.elapsed().as_secs().div(60) as usize >= timeout {
                    warn!("Global timeout expired, request collector stop.");
                    self.kill_tx.blocking_send(true).unwrap();
                }
            }
            // Run stats are only returned when all content has been retrieved,
            // therefore this signals the end of the run.
            if self.check_stats().await {
                break
            }

            // Check if a log came in.
            self.check_results().await;
        }
        self.end_run();
    }

    pub fn end_run(&mut self) {
        self.config.save_known_blobs(&self.known_blobs);
    }

    async fn check_results(&mut self) {

        if let Ok(Some((msg, content))) = self.result_rx.try_next() {
            self.handle_content(msg, content).await;
        }
    }

    async fn handle_content(&mut self, msg: String, content: ContentToRetrieve) {
        self.known_blobs.insert(content.content_id.clone(), content.expiration.clone());
        if let Ok(logs) = serde_json::from_str::<JsonList>(&msg) {
            for log in logs {
                self.handle_log(log, &content).await;
            }
        } else {
            warn!("Skipped log that could not be parsed: {}", content.content_id)
        }
    }

    async fn handle_log(&mut self, mut log: ArbitraryJson, content: &ContentToRetrieve) {

        if let Some(filters) = self.filters.get(&content.content_type) {
            for (k, v) in filters.iter() {
                if let Some(val) = log.get(k) {
                    if val != v {
                        return
                    }
                }
            }
        }
        log.insert("OriginFeed".to_string(),
                   Value::String(content.content_type.to_string()));
        self.cache.insert(log, &content.content_type);
        self.saved += 1;
        if self.cache.full() {
            self.output().await;
        }
    }
    async fn check_stats(&mut self) -> bool {

        if let Ok(Some((found,
                        successful,
                        retried,
                        failed))) = self.stats_rx.try_next() {

            self.output().await;
            let output = self.get_output_string(
                found,
                successful,
                failed,
                retried,
                self.saved,
            );
            info!("{}", output);
            println!("{}", output);
            true
        } else {
            false
        }
    }

    async fn output(&mut self) {

        let mut cache = Caches::new(self.cache.size);
        swap(&mut self.cache, &mut cache);
        if self.interfaces.len() == 1 {
            self.interfaces.get_mut(0).unwrap().send_logs(cache).await;
        } else {
            for interface in self.interfaces.iter_mut() {
                interface.send_logs(cache.clone()).await;
            }
        }
    }

    fn get_output_string(&self, found: usize, successful: usize, failed: usize, retried: usize,
                         saved: usize) -> String {
        format!("\
            Blobs found: {}\n\
            Blobs successful: {}\n\
            Blobs failed: {}\n\
            Blobs retried: {}\n\
            Logs saved: {}\n",
            found, successful, failed, retried, saved
        )
    }

}


/// Initialize a config object for each sub thread to run
/// - Blob thread: Collect available content blobs
/// - Content thread: Collect the blobs found by blob thread
/// - Message loop: Communicates with other threads to handle retries and terminate when finished
fn initialize_channels(
    api: ApiConnection, content_types: ContentTypesSubConfig,
    runs: HashMap<String, Vec<(String, String)>>, config: &Config)
    -> (data_structures::GetBlobConfig,
        data_structures::GetContentConfig,
        data_structures::MessageLoopConfig,
        Receiver<(String, String)>,
        Receiver<ContentToRetrieve>,
        Receiver<(String, ContentToRetrieve)>,
        Receiver<(usize, usize, usize, usize)>,
        tokio::sync::mpsc::Sender<bool>) {

    let urls = api.create_base_urls(runs);

    // Create channels to communicate with async closures
    let (status_tx, status_rx):
        (Sender<data_structures::StatusMessage>,
         Receiver<data_structures::StatusMessage>) = channel(100000);

    let (blobs_tx, blobs_rx):
        (Sender<(String, String)>,
         Receiver<(String, String)>) = channel(100000);

    let (blob_error_tx, blob_error_rx):
        (Sender<(String, String)>,
         Receiver<(String, String)>) = channel(100000);

    let (content_tx, content_rx):
        (Sender<ContentToRetrieve>,
         Receiver<ContentToRetrieve>) = channel(100000);

    let (content_error_tx, content_error_rx):
        (Sender<ContentToRetrieve>,
         Receiver<ContentToRetrieve>) = channel(100000000);

    let (result_tx, result_rx):
        (Sender<(String, ContentToRetrieve)>,
         Receiver<(String, ContentToRetrieve)>) = channel(100000000);

    let (stats_tx, stats_rx):
        (Sender<(usize, usize, usize, usize)>,
         Receiver<(usize, usize, usize, usize)>) = channel(100000000);

    let (kill_tx, kill_rx): (tokio::sync::mpsc::Sender<bool>,
                             tokio::sync::mpsc::Receiver<bool>) = tokio::sync::mpsc::channel(1000);

    let blob_config = data_structures::GetBlobConfig {
        client: reqwest::Client::new(),
        headers: api.headers.clone(),
        status_tx: status_tx.clone(), blobs_tx: blobs_tx.clone(),
        blob_error_tx: blob_error_tx.clone(), content_tx: content_tx.clone(),
        threads: config.collect.max_threads.unwrap_or(50)
    };

    let content_config = data_structures::GetContentConfig {
        client: reqwest::Client::new(),
        headers: api.headers.clone(),
        result_tx: result_tx.clone(),
        content_error_tx: content_error_tx.clone(),
        status_tx: status_tx.clone(),
        threads: config.collect.max_threads.unwrap_or(50)
    };

    let message_loop_config = data_structures::MessageLoopConfig {
        content_tx: content_tx.clone(),
        blobs_tx: blobs_tx.clone(),
        stats_tx: stats_tx.clone(),
        urls,
        content_error_rx,
        status_rx,
        blob_error_rx,
        content_types,
        retries: config.collect.retries.unwrap_or(3),
        kill_rx,
    };
    (blob_config, content_config, message_loop_config, blobs_rx, content_rx, result_rx,
            stats_rx, kill_tx)
}


/// Get all the available log content for a list of content types and runs (start- and end times
/// of content to receive).
fn get_available_content(api: ApiConnection,
                         content_types: ContentTypesSubConfig,
                         runs: HashMap<String, Vec<(String, String)>>,
                         config: &Config,
                         known_blobs: HashMap<String, String>)
                         -> (Receiver<(String, ContentToRetrieve)>,
                             Receiver<(usize, usize, usize, usize)>,
                             tokio::sync::mpsc::Sender<bool>) {

    let (blob_config,
        content_config,
        message_loop_config,
        blobs_rx,
        content_rx,
        result_rx,
        stats_rx,
        kill_tx) = initialize_channels(api, content_types, runs, config);

    spawn_blob_collector(blob_config,
                         content_config,
                         message_loop_config,
                         blobs_rx,
                         content_rx,
                         known_blobs);

    (result_rx, stats_rx, kill_tx)
}


/// Spawn threads running the actual collectors, and a message loop thread to keep track of
/// progress and terminate once finished.
fn spawn_blob_collector(
    blob_config: data_structures::GetBlobConfig,
    content_config: data_structures::GetContentConfig,
    message_loop_config: data_structures::MessageLoopConfig,
    blobs_rx: Receiver<(String, String)>,
    content_rx: Receiver<ContentToRetrieve>,
    known_blobs: HashMap<String, String>) {

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
    let mut awaiting_content_types: usize = 0;
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

        if let Ok(msg) = config.kill_rx.try_recv() {
            if msg {
                info!("Stopping collector.");
                break
            }
        }
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
    }
    // We send back stats after exiting the loop, signalling the end of the run.
    config.stats_tx.send((stats.blobs_found, stats.blobs_successful, stats.blobs_retried,
                          stats.blobs_error)).await.unwrap();
}
