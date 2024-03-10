use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, LineWriter, Read, Write};
use std::path::Path;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_derive::Deserialize;
use crate::data_structures::ArbitraryJson;


#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub log: Option<LogSubConfig>,
    pub collect: CollectSubConfig,
    pub output: OutputSubConfig
}
impl Config {

    pub fn new(path: String) -> Self {

        let open_file = File::open(path)
            .unwrap_or_else(|e| panic!("Config path could not be opened: {}", e.to_string()));
        let reader = BufReader::new(open_file);
        let config: Config = serde_yaml::from_reader(reader)
            .unwrap_or_else(|e| panic!("Config could not be parsed: {}", e.to_string()));
        config
    }

    pub fn get_needed_runs(&self) -> HashMap<String, Vec<(String, String)>> {

        let mut runs: HashMap<String, Vec<(String, String)>> = HashMap::new();
        let end_time = chrono::Utc::now();

        let hours_to_collect = self.collect.hours_to_collect.unwrap_or(24);
        if hours_to_collect > 168 {
            panic!("Hours to collect cannot be more than 168 due to Office API limits");
        }
        for content_type in self.collect.content_types.get_content_type_strings() {
            runs.insert(content_type.clone(), vec!());
            let mut  start_time = end_time - chrono::Duration::try_hours(hours_to_collect)
                .unwrap();

            while end_time - start_time > chrono::Duration::try_hours(24).unwrap() {
                let split_end_time = start_time + chrono::Duration::try_hours(24)
                    .unwrap();
                let formatted_start_time = start_time.format("%Y-%m-%dT%H:%M:%SZ").to_string();
                let formatted_end_time = end_time.format("%Y-%m-%dT%H:%M:%SZ").to_string();
                runs.get_mut(&content_type).unwrap().push((formatted_start_time, formatted_end_time));
                start_time = split_end_time;
            }
            let formatted_start_time = start_time.format("%Y-%m-%dT%H:%M:%SZ").to_string();
            let formatted_end_time = end_time.format("%Y-%m-%dT%H:%M:%SZ").to_string();
            runs.get_mut(&content_type).unwrap().push((formatted_start_time, formatted_end_time));
        }
        runs
    }

    pub fn load_known_blobs(&self) -> HashMap<String, String> {
        let working_dir = if let Some(i) = &self.collect.working_dir {
            i.as_str()
        } else {
            "./"
        };

        let file_name = Path::new("known_blobs");
        let mut path = Path::new(working_dir).join(file_name);
        self.load_known_content(path.as_mut_os_string())
    }

    pub fn save_known_blobs(&mut self, known_blobs: &HashMap<String, String>) {

        let mut known_blobs_path = Path::new(self.collect.working_dir.as_ref()
            .unwrap_or(&"./".to_string())).join(Path::new("known_blobs"));
        self.save_known_content(known_blobs, &known_blobs_path.as_mut_os_string())
    }

    fn load_known_content(&self, path: &OsString) -> HashMap<String, String> {

        let mut known_content = HashMap::new();
        if !Path::new(path).exists() {
            return known_content
        }

        // Load file
        let mut known_content_file = File::open(path).unwrap();
        let mut known_content_string = String::new();
        known_content_file.read_to_string(&mut known_content_string).unwrap();
        for line in known_content_string.lines() {
            if line.trim().is_empty() {
                continue
            }
            // Skip load expired content
            let now = Utc::now();
            if let Some((id, creation_time)) = line.split_once(',') {
                let invalidated = if let Ok(i) =
                    NaiveDateTime::parse_from_str(creation_time, "%Y-%m-%dT%H:%M:%S.%fZ") {
                    let time_utc = DateTime::<Utc>::from_naive_utc_and_offset(i, Utc);
                    now >= time_utc
                } else {
                    true
                };
                if !invalidated {
                    known_content.insert(id.trim().to_string(), creation_time.trim().to_string());
                }
            }
        }
        known_content
    }

    fn save_known_content(&mut self, known_content: &HashMap<String, String>, path: &OsString) {

        let known_content_file = File::create(path).unwrap();
        let mut writer = LineWriter::new(known_content_file);

        for (id, creation_time) in known_content.iter() {
            writer.write_all(format!("{},{}\n", id, creation_time).as_bytes()).unwrap();
        }
        writer.flush().unwrap();
    }

}

#[derive(Deserialize, Clone, Debug)]
pub struct LogSubConfig {
    pub path: String,
    pub debug: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct CollectSubConfig {
    #[serde(rename = "workingDir")]
    pub working_dir: Option<String>,
    #[serde(rename = "cacheSize")]
    pub cache_size: Option<usize>,
    #[serde(rename = "contentTypes")]
    pub content_types: ContentTypesSubConfig,
    #[serde(rename = "maxThreads")]
    pub max_threads: Option<usize>,
    #[serde(rename = "globalTimeout")]
    pub global_timeout: Option<usize>,
    pub retries: Option<usize>,
    #[serde(rename = "hoursToCollect")]
    pub hours_to_collect: Option<i64>,
    #[serde(rename = "skipKnownLogs")]
    pub skip_known_logs: Option<bool>,
    pub filter: Option<FilterSubConfig>,
}
#[derive(Deserialize, Copy, Clone, Debug)]
pub struct ContentTypesSubConfig {
    #[serde(rename = "Audit.General")]
    pub general: Option<bool>,
    #[serde(rename = "Audit.AzureActiveDirectory")]
    pub azure_active_directory: Option<bool>,
    #[serde(rename = "Audit.Exchange")]
    pub exchange: Option<bool>,
    #[serde(rename = "Audit.SharePoint")]
    pub share_point: Option<bool>,
    #[serde(rename = "DLP.All")]
    pub dlp: Option<bool>,
}
impl ContentTypesSubConfig {
    pub fn get_content_type_strings(&self) -> Vec<String> {
        let mut results = Vec::new();
        if self.general.unwrap_or(false) {
            results.push("Audit.General".to_string())
        }
        if self.azure_active_directory.unwrap_or(false) {
            results.push("Audit.AzureActiveDirectory".to_string())
        }
        if self.exchange.unwrap_or(false) {
            results.push("Audit.Exchange".to_string())
        }
        if self.share_point.unwrap_or(false) {
            results.push("Audit.SharePoint".to_string())
        }
        if self.dlp.unwrap_or(false) {
            results.push("DLP.All".to_string())
        }
        results
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct FilterSubConfig {
    #[serde(rename = "Audit.General")]
    pub general: Option<ArbitraryJson>,
    #[serde(rename = "Audit.AzureActiveDirectory")]
    pub azure_active_directory: Option<ArbitraryJson>,
    #[serde(rename = "Audit.Exchange")]
    pub exchange: Option<ArbitraryJson>,
    #[serde(rename = "Audit.SharePoint")]
    pub share_point: Option<ArbitraryJson>,
    #[serde(rename = "DLP.All")]
    pub dlp: Option<ArbitraryJson>,
}
impl FilterSubConfig {
    pub fn get_filters(&self) -> HashMap<String, ArbitraryJson> {

        let mut results = HashMap::new();
        if let Some(filter) = self.general.as_ref() {
            results.insert("Audit.General".to_string(), filter.clone());
        }
        if let Some(filter) = self.azure_active_directory.as_ref() {
            results.insert("Audit.AzureActiveDirectory".to_string(), filter.clone());
        }
        if let Some(filter) = self.share_point.as_ref() {
            results.insert("Audit.SharePoint".to_string(), filter.clone());
        }
        if let Some(filter) = self.exchange.as_ref() {
            results.insert("Audit.Exchange".to_string(), filter.clone());
        }
        if let Some(filter) = self.dlp.as_ref() {
            results.insert("DLP.All".to_string(), filter.clone());
        }
        results
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct OutputSubConfig {
    pub file: Option<FileOutputSubConfig>,
    pub graylog: Option<GraylogOutputSubConfig>,
    pub fluentd: Option<FluentdOutputSubConfig>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct FileOutputSubConfig {
    pub path: String,
    #[serde(rename = "separateByContentType")]
    pub separate_by_content_type: Option<bool>,
    pub separator: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct GraylogOutputSubConfig {
    pub address: String,
    pub port: u16,
}

#[derive(Deserialize, Clone, Debug)]
pub struct FluentdOutputSubConfig {
    #[serde(rename = "tenantName")]
    pub tenant_name: String,
    pub address: String,
    pub port: u16,
}
