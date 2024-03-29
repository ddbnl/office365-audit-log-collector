use std::collections::HashMap;
use std::time::Duration;
use reqwest;
use log::{debug, warn, error, info};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap};
use tokio;
use serde_json;
use futures::{SinkExt, StreamExt};
use futures::channel::mpsc::{Receiver, Sender};
use crate::config::Config;
use crate::data_structures::{JsonList, StatusMessage, GetBlobConfig, GetContentConfig, AuthResult,
                             ContentToRetrieve, CliArgs};
use anyhow::{anyhow, Result};
use serde_json::Value;


/// Return a logged in API connection object. Use the Headers value to make API requests.
pub async fn get_api_connection(args: CliArgs, config: Config) -> Result<ApiConnection> {

    let mut api = ApiConnection {
        args,
        config,
        headers: HeaderMap::new(),
    };
    api.login().await?;
    Ok(api)
}


/// Abstraction of an API connection to Azure Management APIs. Can be used to login to the API
/// which sets the headers. These headers can then be used to make authenticated requests.
#[derive(Clone)]
pub struct ApiConnection {
    pub args: CliArgs,
    pub config: Config,
    pub headers: HeaderMap,
}
impl ApiConnection {
    /// Use tenant_id, client_id and secret_key to request a bearer token and store it in
    /// our headers. Must be called once before requesting any content.
    pub async fn login(&mut self) -> Result<()> {
        info!("Logging in to Office Management API.");
        let auth_url = format!("https://login.microsoftonline.com/{}/oauth2/token",
                               self.args.tenant_id.to_string());

        let resource = "https://manage.office.com";

        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", &self.args.client_id),
            ("client_secret", &self.args.secret_key),
            ("resource", &resource)];

        self.headers.insert(CONTENT_TYPE, "application/x-www-form-urlencoded".parse().unwrap());

        let login_client = reqwest::Client::new();
        let response = login_client
            .post(auth_url)
            .headers(self.headers.clone())
            .form(&params)
            .send()
            .await?;
        if !response.status().is_success() {
            let text = response.text().await?;
            let msg = format!("Received error response to API login: {}", text);
            error!("{}", msg);
            return Err(anyhow!("{}", msg));
        }
        let json = response.json::<AuthResult>().await?;
        let token = format!("bearer {}", json.access_token);
        self.headers.insert(AUTHORIZATION, token.parse().unwrap());
        info!("Successfully logged in to Office Management API.");
        Ok(())
    }

    fn get_base_url(&self) -> String {
        format!("https://manage.office.com/api/v1.0/{}/activity/feed", self.args.tenant_id)
    }

    pub async fn get_feeds(&self) -> Result<Vec<String>> {

        let url = format!("{}/subscriptions/list", self.get_base_url());
        let client = reqwest::Client::new();
        let result: Vec<HashMap<String, Value>> = client
            .get(url)
            .headers(self.headers.clone())
            .header("content-length", 0)
            .send()
            .await?
            .json()
            .await?;
        Ok(result.iter()
            .filter(|x| x.get("status").unwrap() == "enabled")
            .map(|x|x.get("contentType").unwrap().as_str().unwrap().to_string())
            .collect())
    }

    pub async fn set_subscription(&self, content_type: String, enable: bool) -> Result<()> {

        let action = if enable { "start" } else { "stop" };
        let url = format!("{}/subscriptions/{}?contentType={}",
                          self.get_base_url(),
                          action,
                          content_type
        );
        debug!("Subscribing to {} feed.", content_type);
        let client = reqwest::Client::new();
        let response = client
            .post(url)
            .headers(self.headers.clone())
            .header("content-length", 0)
            .send()
            .await?;
        if !response.status().is_success() {
            let text = response.text().await?;
            let msg = format!("Received error response subscribing to audit feed {}: {}", content_type, text);
            error!("{}", msg);
            return Err(anyhow!("{}", msg))
        }
        Ok(())
    }

    pub async fn subscribe_to_feeds(&self) -> Result<()> {

        info!("Subscribing to audit feeds.");
        let mut content_types = self.config.collect.content_types.get_content_type_strings();

        let client = reqwest::Client::new();
        info!("Getting current audit feed subscriptions.");
        let url = format!("{}/subscriptions/list", self.get_base_url());
        let result: Vec<HashMap<String, Value>> = client
            .get(url)
            .headers(self.headers.clone())
            .header("content-length", 0)
            .send()
            .await?
            .json()
            .await?;
        for subscription in result {
            let status = subscription
                .get("status")
                .expect("No status in JSON")
                .as_str()
                .unwrap()
                .to_string()
                .to_lowercase();
            if status == "enabled" {
                let content_type = subscription
                    .get("contentType")
                    .expect("No contentType in JSON")
                    .as_str()
                    .unwrap()
                    .to_string()
                    .to_lowercase();
                if let Some(i) = content_types
                    .iter()
                    .position(|x| x.to_lowercase() == content_type) {
                    info!("Already subscribed to feed {}", content_type);
                    content_types.remove(i);
                }
            }
        }
        for content_type in content_types {
            self.set_subscription(content_type, true).await?;
        }
        info!("All audit feeds subscriptions exist.");
        Ok(())
    }


    /// Create a URL that can retrieve the first page of content for each passed content type. Each
    /// content type can have multiple runs specified. A run consists of a start- and end date to
    /// retrieve data for. Max. time span is 24, so if the user wants to retrieve for e.g. 72 hours,
    /// we need 3 runs of 24 hours each. The runs object looks like e.g.:
    /// Runs{Audit.Exchange: [(start_date, end_date), (start_date, end_date), (start_date, end_date)}
    pub fn create_base_urls(&self, runs: HashMap<String, Vec<(String, String)>>) -> Vec<(String, String)> {

        let mut urls_to_get: Vec<(String, String)> = Vec::new();
        let content_to_get = self.config.collect.content_types.get_content_type_strings();
        for content_type in content_to_get {
            let content_runs = runs.get(&content_type).unwrap();
            for content_run in content_runs.into_iter() {
                let (start_time, end_time) = content_run;
                urls_to_get.push(
                    (content_type.to_string(),
                     format!("{}/subscriptions/content?contentType={}&startTime={}&endTime={}\
                      &PublisherIdentifier={}",
                             self.get_base_url(),
                             content_type,
                             start_time,
                             end_time,
                             self.args.publisher_id)
                    ));
            }
        }
        urls_to_get
    }
}


/// Get available content blobs to retrieve. A base URL receives the initial page of content blobs.
/// The response header could specify 'NextPageUri', which if it exists specifies the URL for the
/// next page of content. This is sent over the blobs_tx channel to retrieve as well. If no
/// additional pages exist, a status message is sent to indicate all content blobs for this
/// content type have been retrieved.
#[tokio::main(flavor="multi_thread", worker_threads=20)]
pub async fn get_content_blobs(config: GetBlobConfig, blobs_rx: Receiver<(String, String)>,
                               known_blobs: HashMap<String, String>) {

    blobs_rx.for_each_concurrent(config.threads, |(content_type, url)| {

        let blobs_tx = config.blobs_tx.clone();
        let blob_error_tx = config.blob_error_tx.clone();
        let mut status_tx = config.status_tx.clone();
        let content_tx = config.content_tx.clone();
        let client = config.client.clone();
        let headers = config.headers.clone();
        let content_type = content_type.clone();
        let url = url.clone();
        let known_blobs = known_blobs.clone();
        let duplicate = config.duplicate;
        async move {
            match client
                .get(url.clone())
                .timeout(Duration::from_secs(5))
                .headers(headers.clone()).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        handle_blob_response(resp, blobs_tx, status_tx, content_tx, blob_error_tx,
                                             content_type, url, &known_blobs, duplicate).await;
                    } else {
                        if let Ok(text) = resp.text().await {
                            if text.to_lowercase().contains("too many request") {
                                status_tx.send(StatusMessage::BeingThrottled).await.unwrap();
                            } else {
                                error!("Err getting blob response {}", text);
                            }
                            handle_blob_response_error(status_tx, blob_error_tx, content_type, url).await;
                        }
                    }
                },
                Err(e) => {
                    error!("Err getting blob response {}", e);
                    handle_blob_response_error(status_tx, blob_error_tx, content_type, url).await;
                }
            }
        }
    }).await;
    debug!("Exit blob thread");
}


/// Deal with the response of a successful content blob request. Try to decode into JSON to
/// retrieve the content URIs of the content inside the blob. Also check response header for another
/// page of content blobs.
async fn handle_blob_response(
    resp: reqwest::Response, blobs_tx: Sender<(String, String)>,
    mut status_tx: Sender<StatusMessage>, content_tx: Sender<ContentToRetrieve>,
    mut blob_error_tx: Sender<(String, String)>, content_type: String, url: String,
    known_blobs: &HashMap<String, String>, duplicate: usize) {

    handle_blob_response_paging(&resp, blobs_tx, status_tx.clone(), content_type.clone()).await;

    match resp.text().await {
        Ok(text) => {
            match serde_json::from_str::<Vec<HashMap<String, Value>>>(text.as_str()) {
                Ok(i) => {
                    handle_blob_response_content_uris(status_tx, content_tx, content_type, i, known_blobs,
                                                      duplicate)
                        .await;
                },
                Err(e) => {
                    warn!("Error getting blob JSON {}", e);
                    debug!("Errored blob json content: {}", text);
                    match blob_error_tx.send((content_type, url)).await {
                        Err(e) => {
                            error!("Could not resend failed blob, dropping it: {}", e);
                            status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap_or_else(
                                |e| panic!("Could not send status update, channel closed?: {}", e)
                            );
                        },
                        _=> (),
                    }
                }
            }
        },
        Err(e) => {
            warn!("Error getting blob response text {}", e);
            match blob_error_tx.send((content_type, url)).await {
                Err(e) => {
                    error!("Could not resend failed blob, dropping it: {}", e);
                    status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap_or_else(
                        |e| panic!("Could not send status update, channel closed?: {}", e)
                    );
                },
                _=> (),
            }
        }
    }
}


/// Determine if a content blob response header contains a reference to another page of blobs.
async fn handle_blob_response_paging(
    resp: &reqwest::Response, mut blobs_tx: Sender<(String, String)>,
    mut status_tx: Sender<StatusMessage>, content_type: String) {

    let next_or_not = resp.headers().get("NextPageUri");
    match next_or_not {
        Some(i) => {
            let new_url = i.to_str().unwrap().to_string();
            blobs_tx.send((content_type.clone(), new_url)).await.unwrap_or_else(
                |e| panic!("Could not send found blob, channel closed?: {}", e)
            );
        },
        None => {
            status_tx.
                send(StatusMessage::FinishedContentBlobs).await.unwrap_or_else(
                    |e| panic!("Could not send status update, channel closed?: {}", e)
            );
        }
    };
}


/// Deal with successfully received and decoded content blobs. Send the URIs of content to retrieve
/// over the content_tx channel for the content thread to retrieve.
async fn handle_blob_response_content_uris(
    mut status_tx: Sender<StatusMessage>, mut content_tx: Sender<ContentToRetrieve>,
    content_type: String, content_json: JsonList, known_blobs: &HashMap<String, String>,
    duplicate: usize) {

    for json_dict in content_json.into_iter() {
        if json_dict.contains_key("contentUri") == false {
            warn!("Invalid blob!: {:?}", json_dict);
        } else {
            let content_id = json_dict.get("contentId").unwrap()
                .to_string()
                .strip_prefix('"').unwrap().strip_suffix('"').unwrap()
                .to_string();
            if known_blobs.contains_key(&content_id) {
                continue
            }
            let url = json_dict
                .get("contentUri").unwrap()
                .to_string()
                .strip_prefix('"').unwrap().strip_suffix('"').unwrap()
                .to_string();
            let expiration = json_dict.get("contentExpiration").unwrap()
                .to_string()
                .strip_prefix('"').unwrap().strip_suffix('"').unwrap()
                .to_string();
            let content_to_retrieve = ContentToRetrieve {
                expiration, content_type: content_type.clone(), content_id, url};

            if duplicate <= 1 {
                content_tx.send(content_to_retrieve).await.unwrap_or_else(
                    |e| panic!("Could not send found content, channel closed?: {}", e));
                status_tx.send(StatusMessage::FoundNewContentBlob).await.unwrap_or_else(
                    |e| panic!("Could not send status update, channel closed?: {}", e));
            } else {
                for _ in 0..duplicate {
                    content_tx.send(content_to_retrieve.clone()).await.unwrap_or_else(
                        |e| panic!("Could not send found content, channel closed?: {}", e));
                    status_tx.send(StatusMessage::FoundNewContentBlob).await.unwrap_or_else(
                        |e| panic!("Could not send status update, channel closed?: {}", e));
                }
            }
        }
    };
}

/// Deal with error while requesting a content blob.
async fn handle_blob_response_error(
        mut status_tx: Sender<StatusMessage>, mut blob_error_tx: Sender<(String, String)>,
        content_type: String, url: String) {

    match blob_error_tx.send((content_type, url)).await {
        Err(e) => {
            error!("Could not resend failed blob, dropping it: {}", e);
            status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap_or_else(
                |e| panic!("Could not send status update, channel closed?: {}", e)
            );
        },
        _=> (),
    }
}


/// Retrieve the actual ContentUris found in the JSON body of content blobs.
#[tokio::main(flavor="multi_thread", worker_threads=50)]
pub async fn get_content(config: GetContentConfig, content_rx: Receiver<ContentToRetrieve>) {

    content_rx.for_each_concurrent(config.threads, |content_to_retrieve| {
        let client = config.client.clone();
        let headers = config.headers.clone();
        let result_tx = config.result_tx.clone();
        let status_tx = config.status_tx.clone();
        let content_error_tx = config.content_error_tx.clone();
        async move {
            match client.get(content_to_retrieve.url.clone())
                .timeout(Duration::from_secs(3))
                .headers(headers)
                .send()
                .await {
                Ok(resp) => {
                    handle_content_response(resp, result_tx, status_tx, content_error_tx,
                    content_to_retrieve).await;
                },
                Err(_) => {
                    handle_content_response_error(status_tx, content_error_tx, content_to_retrieve)
                        .await;
                }
            }
        }
    }).await;
    info!("Exit content thread");
}


/// Deal with successful content request response.
async fn handle_content_response(
    resp: reqwest::Response, mut result_tx: Sender<(String, ContentToRetrieve)>,
    mut status_tx: Sender<StatusMessage>, mut content_error_tx: Sender<ContentToRetrieve>,
    content_to_retrieve: ContentToRetrieve) {

    if !resp.status().is_success() {
        match content_error_tx.send(content_to_retrieve).await {
            Err(_) => {
                status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap_or_else(
                    |e| panic!("Could not send status update, channel closed?: {}", e)
                );
            },
            _=> (),
        }
        if let Ok(text) = resp.text().await {
            if text.to_lowercase().contains("too many request") {
                match status_tx.send(StatusMessage::BeingThrottled).await {
                    Err(e) => {
                        error!("Could not send status message: {}", e);
                    },
                    _=> (),
                }
            }
        }
        return
    }

    match resp.text().await {
        Ok(json) => {
            result_tx.send((json, content_to_retrieve)).await.unwrap_or_else(
                |e| panic!("Could not send status update, channel closed?: {}", e)
            );
            status_tx.send(StatusMessage::RetrievedContentBlob).await.unwrap();
        }
        Err(e) => {
            warn!("Error interpreting JSON: {}", e);
            match content_error_tx.send(content_to_retrieve).await {
                Err(_) => {
                    status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap_or_else(
                        |e| panic!("Could not send status update, channel closed?: {}", e)
                    );
                },
                _=> (),
            }
        }
    }
}


/// Deal with error response requesting a contentURI.
async fn handle_content_response_error(
    mut status_tx: Sender<StatusMessage>, mut content_error_tx: Sender<ContentToRetrieve>,
    content_to_retrieve: ContentToRetrieve) {

        match content_error_tx.send(content_to_retrieve).await {
        Err(e) => {
            error!("Could not resend failed content, dropping it: {}", e);
            status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap_or_else(
                |e| panic!("Could not send status update, channel closed?: {}", e)
            );
        },
        _=> (),
    }
}
