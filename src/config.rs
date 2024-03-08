use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{LineWriter, Read, Write};
use std::path::Path;
use serde_derive::Deserialize;
use serde_json::Value;


#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub log: Option<LogSubConfig>,
    pub collect: CollectSubConfig,
    pub output: OutputSubConfig
}
impl Config {

    pub fn get_needed_runs(&self) -> HashMap<String, Vec<(String, String)>> {

        let mut runs: HashMap<String, Vec<(String, String)>> = HashMap::new();
        let end_time = chrono::Utc::now();

        let hours_to_collect = self.collect.hoursToCollect.unwrap_or(24);
        if hours_to_collect > 168 {
            panic!("Hours to collect cannot be more than 168 due to Office API limits");
        }
        for content_type in self.collect.contentTypes.get_content_type_strings() {
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

        let mut known_blobs_path = Path::new(self.collect.workingDir.as_ref()
            .unwrap_or(&"./".to_string())).join(Path::new("known_blobs"));
        self.load_known_content(known_blobs_path.as_mut_os_string())
    }

    pub fn load_known_logs(&self) -> HashMap<String, String> {

        let mut known_logs_path = Path::new(self.collect.workingDir.as_ref()
            .unwrap_or(&"./".to_string())).join(Path::new("known_logs"));
        self.load_known_content(&known_logs_path.as_mut_os_string())
    }

    pub fn save_known_blobs(&mut self, known_blobs: &HashMap<String, String>) {

        let mut known_blobs_path = Path::new(self.collect.workingDir.as_ref()
            .unwrap_or(&"./".to_string())).join(Path::new("known_blobs"));
        self.save_known_content(known_blobs, &known_blobs_path.as_mut_os_string())
    }

    pub fn save_known_logs(&mut self, known_logs: &HashMap<String, String>) {

        let mut known_logs_path = Path::new(self.collect.workingDir.as_ref()
            .unwrap_or(&"./".to_string())).join(Path::new("known_logs"));
        self.save_known_content(known_logs, &known_logs_path.as_mut_os_string())
    }
    fn load_known_content(&self, path: &OsString) -> HashMap<String, String> {

        let mut known_content = HashMap::new();

        if !Path::new(path).exists() {
            return known_content
        }

        let mut known_content_file = File::open(path).unwrap();
        let mut known_content_string = String::new();
        known_content_file.read_to_string(&mut known_content_string).unwrap();
        for line in known_content_string.lines() {
            if line.trim().is_empty() {
                continue
            }
            if let Some((id, creation_time)) = line.split_once(',') {
                known_content.insert(id.trim().to_string(), creation_time.trim().to_string());
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
    pub workingDir: Option<String>,
    pub cacheSize: Option<usize>,
    pub contentTypes: ContentTypesSubConfig,
    pub maxThreads: Option<usize>,
    pub globalTimeout: Option<usize>,
    pub retries: Option<usize>,
    pub retryCooldown: Option<usize>,
    pub autoSubscribe: Option<bool>,  // Deprecated
    pub resume: Option<bool>,  // Deprecated
    pub hoursToCollect: Option<i64>,
    pub skipKnownLogs: Option<bool>,
    pub filter: Option<FilterSubConfig>,
}
#[derive(Deserialize, Copy, Clone, Debug)]
pub struct ContentTypesSubConfig {
    #[serde(rename = "Audit.General")]
    pub general: bool,
    #[serde(rename = "Audit.AzureActiveDirectory")]
    pub azureActiveDirectory: bool,
    #[serde(rename = "Audit.Exchange")]
    pub exchange: bool,
    #[serde(rename = "Audit.SharePoint")]
    pub sharePoint: bool,
    #[serde(rename = "DLP.All")]
    pub dlp: bool,
}
impl ContentTypesSubConfig {
    pub fn get_content_type_strings(&self) -> Vec<String> {
        let mut results = Vec::new();
        if self.general {
            results.push("Audit.General".to_string())
        }
        if self.azureActiveDirectory {
            results.push("Audit.AzureActiveDirectory".to_string())
        }
        if self.exchange {
            results.push("Audit.Exchange".to_string())
        }
        if self.sharePoint {
            results.push("Audit.SharePoint".to_string())
        }
        if self.dlp {
            results.push("DLP.All".to_string())
        }
        results
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct FilterSubConfig {
    #[serde(rename = "Audit.General")]
    pub general: Option<HashMap<String, Value>>,
    #[serde(rename = "Audit.AzureActiveDirectory")]
    pub azureActiveDirectory: Option<HashMap<String, Value>>,
    #[serde(rename = "Audit.Exchange")]
    exchange: Option<HashMap<String, Value>>,
    #[serde(rename = "Audit.SharePoint")]
    pub sharePoint: Option<HashMap<String, Value>>,
    #[serde(rename = "DLP.All")]
    pub dlp: Option<HashMap<String, Value>>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct OutputSubConfig {
    pub file: Option<FileOutputSubConfig>,
    pub azureLogAnalytics: Option<OmsOutputSubConfig>,
    pub azureTable: Option<AzTableOutputSubConfig>,
    pub azureBlob: Option<AzBlobOutputSubConfig>,
    pub sql: Option<sqlOutputSubConfig>,
    pub graylog: Option<GraylogOutputSubConfig>,
    pub fluentd: Option<FluentdOutputSubConfig>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct FileOutputSubConfig {
    pub path: String,
    pub separateByContentType: Option<bool>,
    pub separator: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct OmsOutputSubConfig {
    pub workspaceId: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct AzTableOutputSubConfig {
    pub tableName: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct AzBlobOutputSubConfig {
    pub containerName: String,
    pub blobName: String,
    pub tempPath: Option<String>,
    pub separateByContentType: Option<bool>,
    pub separator: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct sqlOutputSubConfig {
    pub chunkSize: Option<usize>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct GraylogOutputSubConfig {
    pub address: String,
    pub port: usize,
}

#[derive(Deserialize, Clone, Debug)]
pub struct FluentdOutputSubConfig {
    pub tenantName: String,
    pub address: String,
    pub port: usize,
}
