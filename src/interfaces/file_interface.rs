use std::collections::HashMap;
use std::path::Path;
use chrono::Utc;
use csv::{Writer};
use crate::config::Config;
use crate::data_structures::{ArbitraryJson, Caches};
use crate::interfaces::interface::Interface;

/// Interface that send found logs to one CSV file, or one CSV file per content type.
pub struct FileInterface {
    config: Config,
    paths: HashMap<String, String>,
    postfix: String,
}

impl FileInterface {
    pub fn new(config: Config) -> Self {

        let postfix = Utc::now().format("%Y%m%d%H%M%S").to_string();
        let mut interface = FileInterface {
            config,
            paths: HashMap::new(),
            postfix: postfix.clone()
        };
        if interface.separate_by_content_type() {
            interface.create_content_type_paths();
        }
        interface
    }

    /// Based on the desired CSV path, create a path for each content type. Used
    /// when SeparateByContentType is true.
    fn create_content_type_paths(&mut self) {
        let path = Path::new(&self.config.output.file
            .as_ref()
            .unwrap()
            .path);
        let dir = path.parent();
        let stem = path
            .file_stem().unwrap()
            .to_str().unwrap()
            .to_string();

        let content_strings = self.config.collect.content_types.get_content_type_strings();
        for content_type in  content_strings {
            let mut file = format!("{}_{}_{}.csv",
                               self.postfix.clone(),
                               stem.clone(),
                               content_type.replace('.', ""));
            if let Some(parent) = dir {
                file = format!("{}/{}", parent.to_str().unwrap(), file);
            }
            self.paths.insert(content_type, file);
        }
    }

    /// Convenience method to get config property.
    fn separate_by_content_type(&self) -> bool {
        self.config.output.file.as_ref().unwrap().separate_by_content_type.unwrap_or(false)
    }

    /// Save the logs of all content types in a single CSV file.
    fn send_logs_unified(&self, mut cache: Caches) {

        // Get columns from all content types
        let mut all_logs = cache.get_all();
        let mut columns: Vec<String> = Vec::new();
        for content_type in all_logs.iter_mut() {
            columns.append(&mut get_all_columns(content_type));
        }

        let mut wrt =
            Writer::from_path(&self.config.output.file.as_ref().unwrap().path).unwrap();
        wrt.write_record(&columns).unwrap();
        for logs in all_logs.iter_mut() {
            for log in logs.iter_mut() {
                let new_log = fill_log(log, &columns);
                wrt.write_record(new_log).unwrap();
            }
        }
        wrt.flush().unwrap();
    }

    /// Save the logs of each content type to a separate CSV file.
    fn send_logs_separated(&self, cache: Caches) {
        for (content_type, logs) in cache.get_all_types() {
            if logs.is_empty() {
                continue
            }
            let columns = get_all_columns(logs);
            let path = self.paths.get(&content_type).unwrap();
            let mut wrt = Writer::from_path(path).unwrap();
            wrt.write_record(&columns).unwrap();

            for log in logs {
                let new_log = fill_log(log, &columns);
                wrt.write_record(new_log).unwrap();
            }
            wrt.flush().unwrap();
        }
    }
}

impl Interface for FileInterface {
    fn send_logs(&mut self, logs: Caches) {
        if !self.separate_by_content_type() {
            self.send_logs_unified(logs);
        } else {
            self.send_logs_separated(logs);
        }
    }
}


/// Get all column names in a heterogeneous collection of logs.
fn get_all_columns(logs: &[ArbitraryJson]) -> Vec<String> {

    let mut columns: Vec<String> = Vec::new();
    for log in logs.iter() {
        for k in log.keys() {
            if !columns.contains(k) {
                columns.push(k.to_string());
            }
        }
    }
    columns
}

/// Due to heterogeneous logs not all logs have all columns. Fill missing columns of
/// a log with an empty string.
fn fill_log(log: &ArbitraryJson, columns: &Vec<String>) -> Vec<String> {
    let mut new_log= Vec::new();
    for col in columns {
        if !log.contains_key(col) {
            new_log.push("".to_string());
        } else {
            new_log.push(log.get(col).unwrap().to_string())
        }
    }
    new_log
}