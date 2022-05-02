use std::collections::HashMap;
use reqwest;
use log::{debug, info, warn, error};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap};
use tokio;
use serde_json;
use chrono::{DateTime};
use futures::{SinkExt, StreamExt};
use futures::channel::mpsc::{Receiver, Sender};
use crate::data_structures::{JsonList, StatusMessage, GetBlobConfig, GetContentConfig,
                             AuthResult, ContentToRetrieve};


/// Return a logged in API connection object. Use the Headers value to make API requests.
pub fn get_api_connection(tenant_id: String, client_id: String, secret_key: String,
                          publisher_id: String) -> ApiConnection {

    let mut api = ApiConnection {
        tenant_id,
        client_id,
        secret_key,
        publisher_id,
        headers: HeaderMap::new(),
    };
    api.login();
    api
}
/// Abstraction of an API connection to Azure Management APIs. Can be used to login to the API
/// which sets the headers. These headers can then be used to make authenticated requests.
pub struct ApiConnection {
    pub tenant_id: String,
    pub client_id: String,
    secret_key: String,
    pub publisher_id: String,
    pub headers: HeaderMap,
}
impl ApiConnection {
    /// Use tenant_id, client_id and secret_key to request a bearer token and store it in
    /// our headers. Must be called once before requesting any content.
    fn login(&mut self) {
        let auth_url =
            format!("https://login.microsoftonline.com/{}/oauth2/token",
                    self.tenant_id.to_string());
        let resource = "https://manage.office.com";
        let params = [("grant_type", "client_credentials"), ("client_id", &self.client_id),
            ("client_secret", &self.secret_key), ("resource", &resource)];
        self.headers.insert(CONTENT_TYPE,
                            "application/x-www-form-urlencoded".parse().unwrap());
        let login_client = reqwest::blocking::Client::new();
        let json: AuthResult = login_client
            .post(auth_url)
            .headers(self.headers.clone())
            .form(&params)
            .send()
            .unwrap()
            .json()
            .unwrap();
        let token = format!("bearer {}", json.access_token);
        self.headers.insert(AUTHORIZATION, token.parse().unwrap());
    }
}


/// Create a URL that can retrieve the first page of content for each passed content type. Each
/// content type can have multiple runs specified. A run consists of a start- and end date to
/// retrieve data for. Max. time span is 24, so if the user wants to retrieve for e.g. 72 hours,
/// we need 3 runs of 24 hours each. The runs object looks like e.g.:
/// Runs{Audit.Exchange: [(start_date, end_date), (start_date, end_date), (start_date, end_date)}
pub fn create_base_urls(
    content_types: Vec<String>, tenant_id: String, publisher_id: String,
    runs: HashMap<String, Vec<(String, String)>>) -> Vec<(String, String)> {
    let mut urls_to_get: Vec<(String, String)> = Vec::new();
    for content_type in content_types.iter() {
        let content_runs = runs.get(content_type).unwrap();
        for content_run in content_runs.into_iter() {
            let (start_time, end_time) = content_run;
            urls_to_get.push(
                (content_type.to_string(),
                 format!("https://manage.office.com/api/v1.0/{}/activity/feed/\
                  subscriptions/content?contentType={}&startTime={}&endTime={}\
                  &PublisherIdentifier={}",
                         tenant_id, content_type, start_time, end_time, publisher_id)
                ));
        }
    }
    urls_to_get
}


/// Get available content blobs to retrieve. A base URL receices the initial page of content blobs.
/// The response header could specify 'NextPageUri', which if it exists specifies the URL for the
/// next page of content. This is sent over the blobs_tx channel to retrieve as well. If no
/// additional pages exist, a status message is sent to indicate all content blobs for this
/// content type have been retrieved.
#[tokio::main(flavor="multi_thread", worker_threads=200)]
pub async fn get_content_blobs(config: GetBlobConfig, blobs_rx: Receiver<(String, String)>) {
    blobs_rx.for_each_concurrent(config.threads, |(content_type, url)| {
        let blobs_tx = config.blobs_tx.clone();
        let blob_error_tx = config.blob_error_tx.clone();
        let status_tx = config.status_tx.clone();
        let content_tx = config.content_tx.clone();
        let client = config.client.clone();
        let headers = config.headers.clone();
        let content_type = content_type.clone();
        let url = url.clone();
        async move {
            match client.get(url.clone()).timeout(std::time::Duration::from_secs(5)).
                headers(headers.clone()).send().await {
                Ok(resp) => {
                    handle_blob_response(resp, blobs_tx, status_tx, content_tx, blob_error_tx,
                    content_type, url).await;
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
    mut blob_error_tx: Sender<(String, String)>, content_type: String, url: String) {

    handle_blob_response_paging(&resp, blobs_tx, status_tx.clone(),
                                content_type.clone()).await;
    match resp.json::<Vec<HashMap<String, serde_json::Value>>>().await {
        Ok(i) => {
            handle_blob_response_content_uris(status_tx, content_tx, content_type, i)
                .await;
        },
        Err(e) => {
            warn!("Err getting blob JSON {}", e);
            match blob_error_tx.send((content_type, url)).await {
                Err(e) => {
                    error!("Could not resend failed blob, dropping it: {}", e);
                    status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap();
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
            blobs_tx.send((content_type.clone(), new_url)).await.unwrap();
        },
        None => {
            status_tx.
                send(StatusMessage::FinishedContentBlobs).await.unwrap();
        }
    };
}


/// Deal with successfully received and decoded content blobs. Send the URIs of content to retrieve
/// over the content_tx channel for the content thread to retrieve.
async fn handle_blob_response_content_uris(
    mut status_tx: Sender<StatusMessage>, mut content_tx: Sender<ContentToRetrieve>,
    content_type: String, content_json: JsonList) {

    for json_dict in content_json.into_iter() {
        if json_dict.contains_key("contentUri") == false {
            warn!("Invalid blob!: {:?}", json_dict);
        } else {
            let url = json_dict
                .get("contentUri").unwrap()
                .to_string()
                .strip_prefix('"').unwrap().strip_suffix('"').unwrap()
                .to_string();
            let expiration = json_dict.get("contentExpiration").unwrap()
                .to_string()
                .strip_prefix('"').unwrap().strip_suffix('"').unwrap()
                .to_string();
            let content_id = json_dict.get("contentId").unwrap()
                .to_string()
                .strip_prefix('"').unwrap().strip_suffix('"').unwrap()
                .to_string();
            let content_to_retrieve = ContentToRetrieve {
                expiration, content_type: content_type.clone(), content_id, url};

            content_tx.send(content_to_retrieve).await.unwrap();
            status_tx.send(StatusMessage::FoundNewContentBlob).await.unwrap();
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
            status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap();
        },
        _=> (),
    }
}


/// Retrieve the actual ContentUris found in the JSON body of content blobs.
#[tokio::main(flavor="multi_thread", worker_threads=200)]
pub async fn get_content(config: GetContentConfig, content_rx: Receiver<ContentToRetrieve>) {
    content_rx.for_each_concurrent(config.threads, |content_to_retrieve| {
        let client = config.client.clone();
        let headers = config.headers.clone();
        let result_tx = config.result_tx.clone();
        let status_tx = config.status_tx.clone();
        let content_error_tx = config.content_error_tx.clone();
        async move {
            match client.get(content_to_retrieve.url.clone())
                .timeout(std::time::Duration::from_secs(5)).headers(headers).send().await {
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
    debug!("Exit content thread");
}


/// Deal with successful content request response.
async fn handle_content_response(
    resp: reqwest::Response, result_tx: std::sync::mpsc::Sender<(String, ContentToRetrieve)>,
    mut status_tx: Sender<StatusMessage>, mut content_error_tx: Sender<ContentToRetrieve>,
    content_to_retrieve: ContentToRetrieve) {

    match resp.text().await {
        Ok(json) => {
            result_tx.send((json, content_to_retrieve)).unwrap();
            status_tx.send(StatusMessage::RetrievedContentBlob).await.unwrap();
        }
        Err(e) => {
            warn!("Error interpreting JSON: {}", e);
            match content_error_tx.send(content_to_retrieve).await {
                Err(e) => {
                    error!("Could not resend failed content, dropping it: {}", e);
                    status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap();
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
            status_tx.send(StatusMessage::ErrorContentBlob).await.unwrap();
        },
        _=> (),
    }
}
