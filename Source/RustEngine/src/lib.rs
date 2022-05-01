use std::thread;
use std::collections::HashMap;
use log::{debug, info, warn, error};
use futures::{SinkExt};
use futures::channel::mpsc::channel;
use futures::channel::mpsc::{Sender, Receiver};
use pyo3::prelude::*;
use crate::data_structures::{ContentToRetrieve, RunStatistics};


mod api_connection;
mod data_structures;

#[pyclass]
pub struct RustEngine {
    tenant_id: String,
    client_id: String,
    secret_key: String,
    publisher_id: String,
    content_types: Vec<String>,
    runs: HashMap<String, Vec<(String, String)>>,
    result_rx: Option<std::sync::mpsc::Receiver<(String, ContentToRetrieve)>>,
    stats_rx: Option<std::sync::mpsc::Receiver<(usize, usize, usize, usize)>>,
    threads: usize,
    retries: usize,
}

#[pymethods]
impl RustEngine {

    #[new]
    pub fn new(tenant_id: String, client_id: String, secret_key:String, publisher_id: String,
               content_types: Vec<String>, runs: HashMap<String, Vec<(String, String),>>,
               threads: usize, retries: usize)
               -> RustEngine {
        RustEngine {
            result_rx: None,
            stats_rx: None,
            tenant_id,
            client_id,
            secret_key,
            publisher_id,
            content_types,
            runs,
            threads,
            retries,
        }
    }

    pub fn run_once(&mut self) {
        let api = api_connection::get_api_connection(
            self.tenant_id.clone(), self.client_id.clone(),
            self.secret_key.clone(), self.publisher_id.clone());
        let (result_rx, stats_rx) = get_available_content(
            api, self.content_types.clone(), self.runs.clone(), self.threads,
            self.retries);
        self.result_rx = Some(result_rx);
        self.stats_rx = Some(stats_rx);
    }

    pub fn get_result(&self) -> PyResult<(String, String, String, String)> {
        match self.result_rx.as_ref().unwrap().try_recv() {
            Ok((i,j) ) => Ok((i, j.content_id, j.expiration, j.content_type)),
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                Err(pyo3::exceptions::PyValueError::new_err("No logs ready"))
            },
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                Err(pyo3::exceptions::PyEOFError::new_err("Finished run"))
            }
        }
    }

    pub fn stop(&self) -> PyResult<(usize, usize, usize, usize)> {
        Ok(self.stats_rx.as_ref().unwrap().try_recv().unwrap())
    }
}


/// # Initialize a config object for each sub thread to run
/// - Blob thread: Collect available content blobs
/// - Content thread: Collect the blobs found by blob thread
/// - Message loop: Communicates with other threads to handle retries and terminate when finished
fn initialize_configs(
    api: api_connection::ApiConnection, content_types: Vec<String>,
    runs: HashMap<String, Vec<(String, String)>>, retries: usize, threads:usize)
    -> (data_structures::GetBlobConfig, data_structures::GetContentConfig,
        data_structures::MessageLoopConfig, Receiver<(String, String)>, Receiver<ContentToRetrieve>,
        std::sync::mpsc::Receiver<(String, ContentToRetrieve)>, std::sync::mpsc::Receiver<(usize, usize, usize, usize)>) {

    let urls = api_connection::create_base_urls(
        content_types.clone(), api.tenant_id, api.publisher_id, runs);

    // Create channels to communicate with async closures
    let (status_tx, status_rx): (Sender<data_structures::StatusMessage>, Receiver<data_structures::StatusMessage>) = channel(100000);
    let (blobs_tx, blobs_rx): (Sender<(String, String)>, Receiver<(String, String)>) = channel(100000);
    let (blob_error_tx, blob_error_rx):
        (Sender<(String, String)>, Receiver<(String, String)>) = channel(100000);
    let (content_tx, content_rx): (Sender<ContentToRetrieve>, Receiver<ContentToRetrieve>) = channel(100000);
    let (content_error_tx, content_error_rx): (Sender<ContentToRetrieve>, Receiver<ContentToRetrieve>) = channel(100000000);
    let (result_tx, result_rx):
        (std::sync::mpsc::Sender<(String, ContentToRetrieve)>, std::sync::mpsc::Receiver<(String, ContentToRetrieve)>) =
        std::sync::mpsc::channel();
    let (stats_tx, stats_rx):
        (std::sync::mpsc::Sender<(usize, usize, usize, usize)>, std::sync::mpsc::Receiver<(usize, usize, usize, usize)>) =
        std::sync::mpsc::channel();

    let blob_config = data_structures::GetBlobConfig { client: reqwest::Client::new(), headers: api.headers.clone(),
        status_tx: status_tx.clone(), blobs_tx: blobs_tx.clone(),
        blob_error_tx: blob_error_tx.clone(), content_tx: content_tx.clone(), threads
    };

    let content_config = data_structures::GetContentConfig {
        client: reqwest::Client::new(), headers: api.headers.clone(), result_tx: result_tx.clone(),
        content_error_tx: content_error_tx.clone(), status_tx: status_tx.clone(), threads
    };

    let message_loop_config = data_structures::MessageLoopConfig {
        content_tx: content_tx.clone(), blobs_tx: blobs_tx.clone(), stats_tx: stats_tx.clone(),
        urls, content_error_rx, status_rx, blob_error_rx, content_types, retries};
    return (blob_config, content_config, message_loop_config, blobs_rx, content_rx, result_rx, stats_rx)
}


/// Get all the available log content for a list of content types.
fn get_available_content(api: api_connection::ApiConnection, content_types: Vec<String>,
                         runs: HashMap<String, Vec<(String, String)>>, threads: usize,
                         retries: usize)
    -> (std::sync::mpsc::Receiver<(String, ContentToRetrieve)>,
        std::sync::mpsc::Receiver<(usize, usize, usize, usize)>) {

    let (blob_config, content_config, message_loop_config,
    blobs_rx, content_rx, result_rx, stats_rx)
        = initialize_configs(api, content_types, runs, retries, threads);
    let (blob_handle, content_handle, message_loop_handle)
        = spawn_blob_collector(blob_config, content_config, message_loop_config, blobs_rx,
                               content_rx);
    (result_rx, stats_rx)
}

/// Spawn a thread running the actual collect. This allows the main thread to keep track of
/// statistics and handle any errors in the collector and/or output interfaces.
fn spawn_blob_collector(
    blob_config: data_structures::GetBlobConfig, content_config: data_structures::GetContentConfig,
    message_loop_config: data_structures::MessageLoopConfig, blobs_rx: Receiver<(String, String)>,
    content_rx: Receiver<(ContentToRetrieve)>)
    -> (std::thread::JoinHandle<()>, std::thread::JoinHandle<()>, std::thread::JoinHandle<()>) {

    let blob_handle = thread::spawn( move || {
        api_connection::get_content_blobs(blob_config, blobs_rx);
    });
    let content_handle = thread::spawn( move || {
        api_connection::get_content(content_config, content_rx);
    });

    let msg_loop_handle = thread::spawn(move || {
        message_loop(message_loop_config)
    });

    (blob_handle, content_handle, msg_loop_handle)
}

/// Start receiving messages from the status and stats channels. Status channel will send a
/// termination signal once the async closures are done retrieving content. Stats channels
/// receives updates on how the async closures are doing while running.
#[tokio::main]
pub async fn message_loop(mut config: data_structures::MessageLoopConfig) {

    // Send base URLS for content blob retrieval then keep track of when they've all come in
    let mut awaiting_content_types:usize = 0;
    for (content_type, base_url) in config.urls.into_iter() {
        config.blobs_tx.clone().send((content_type, base_url)).await.unwrap();
        awaiting_content_types += 1;
    }
    let mut awaiting_content_blobs: usize = 0;  // Incremented and decremented by loop
    let mut retry_map :HashMap<String, usize> = HashMap::new();
    let mut stats = RunStatistics::new();
    loop {
        match config.status_rx.try_next() {
            Ok(Some(msg)) => {
                match msg {
                    data_structures::StatusMessage::FinishedContentBlobs => {
                        if awaiting_content_types == 0 {
                        } else {
                            awaiting_content_types -= 1;
                        }
                    },
                    data_structures::StatusMessage::FoundNewContentBlob => {
                        awaiting_content_blobs +=1;
                        stats.blobs_found += 1;
                    },
                    data_structures::StatusMessage::RetrievedContentBlob => {
                        awaiting_content_blobs -= 1;
                        stats.blobs_successful += 1;
                        if awaiting_content_types == 0 && awaiting_content_blobs == 0 {
                            config.content_tx.close_channel();
                            break;
                        }
                    },
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
            },
            _ => ()
        }
        match config.blob_error_rx.try_next() {
            Ok(Some((content_type, url))) => {
                if retry_map.contains_key(&url) == true {
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
            },
            _ => (),
        };
        match config.content_error_rx.try_next() {
            Ok(Some(content)) => {
                if retry_map.contains_key(&content.url) == true {
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
            _ => (),
        }
        /*
        print!("{esc}[2J{esc}[1;1H", esc = 27 as char);
        println!{"Pending content types: {}, Pending content blobs: {}",
                 awaiting_content_types, awaiting_content_blobs}
        */
    }
    config.stats_tx.send((stats.blobs_found, stats.blobs_successful, stats.blobs_retried,
                          stats.blobs_error)).unwrap();
}

#[pymodule]
fn alc(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<RustEngine>()?;
    Ok(())
}
