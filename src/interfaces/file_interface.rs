use std::collections::HashMap;
use std::path::Path;
use chrono::Utc;
use csv::{StringRecord, Writer};
use log::warn;
use serde_json::Value;
use crate::config::Config;
use crate::data_structures::Caches;
use crate::interface::Interface;

pub struct FileInterface {
    config: Config,
    paths: HashMap<String, String>,
    postfix: String,
}
impl FileInterface {
    pub fn new(config: Config) -> Self {

        let mut paths: HashMap<String, String> = HashMap::new();
        let postfix = Utc::now().format("%Y%m%d%H%M%S").to_string();
        let mut interface = FileInterface {
            config,
            paths,
            postfix: postfix.clone()
        };
        if interface.separate_by_content_type() {
            let stem = Path::new(
                &interface.config.output.file.as_ref().unwrap().path.clone())
                .file_stem().unwrap().to_str().unwrap().to_string();
            for content_type in interface.config.collect.contentTypes.get_content_type_strings() {
                let file = format!("{}_{}_{}.csv", postfix.clone(), stem.clone(),
                                   content_type.replace('.', ""));
                interface.paths.insert(content_type, file);
            }
        }
        interface
    }
}

impl Interface for FileInterface {
    fn send_logs(&mut self, cache: Caches) {
        if !self.separate_by_content_type() {
            self.send_logs_unified(cache);
        } else {
            self.send_logs_separated(cache);
        }
    }
}

impl FileInterface {

    fn separate_by_content_type(&self) -> bool {
        self.config.output.file.as_ref().unwrap().separateByContentType.unwrap_or(false)
    }

    fn send_logs_unified(&self, cache: Caches) {

        // Get columns from all content types
        let all_logs = cache.get_all();
        let mut columns: Vec<String> = Vec::new();
        for content_type in all_logs {
            columns.append(&mut get_all_columns(content_type));
        }
        let mut wrt =
            Writer::from_path(&self.config.output.file.as_ref().unwrap().path).unwrap();
        wrt.write_record(&columns).unwrap();
        for logs in all_logs {
            for log in logs {
                let new_log = fill_log(log, &columns);
                wrt.write_record(new_log).unwrap();
            }
        }
        wrt.flush().unwrap();
    }

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


fn get_all_columns(logs: &[HashMap<String, Value>]) -> Vec<String> {

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

fn fill_log(log: &HashMap<String, Value>, columns: &Vec<String>) -> Vec<String> {
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