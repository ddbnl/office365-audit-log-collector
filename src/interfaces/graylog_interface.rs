use std::io::{ErrorKind, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use log::{warn};
use serde_json::Value;
use crate::config::Config;
use crate::data_structures::{ArbitraryJson, Caches};
use crate::interfaces::interface::Interface;

pub struct GraylogInterface {
    address: String,
    port: u16,
}

impl GraylogInterface {

    pub fn new(config: Config) -> Self {

        let address = config.output.graylog.as_ref().unwrap().address.clone();
        let port = config.output.graylog.as_ref().unwrap().port;
        let interface = GraylogInterface {
            address,
            port
        };

        // Test socket, if we cannot connect there's no point in running
        let _ = interface.get_socket();
        interface
    }
}

impl GraylogInterface {
    fn get_socket(&self) -> TcpStream {

        let ip_addr = (self.address.clone(), self.port)
            .to_socket_addrs()
            .expect("Unable to resolve the IP address")
            .next()
            .expect("DNS resolution returned no IP addresses");
        TcpStream::connect_timeout(&ip_addr, Duration::from_secs(10)).unwrap_or_else(
            |e| panic!("Could not connect to Graylog interface on: {}:{} with: {}",
                       self.address, self.port, e)
        )
    }
}

#[async_trait]
impl Interface for GraylogInterface {

    async fn send_logs(&mut self, mut logs: Caches) {

        let mut all_logs = logs.get_all();
        for logs in all_logs.iter_mut() {
            for log in logs.iter_mut() {

                match add_timestamp_field(log) {
                    Ok(()) => (),
                    Err(e) => {
                        warn!("Could parse timestamp for log in Graylog interface: {}", e);
                        continue
                    }
                }

                match serde_json::to_string(log) {
                    Ok(json) => {
                        let mut socket = self.get_socket();
                        socket.write_all(&json.as_bytes()).unwrap_or_else(
                            |e| warn!("Could not send log to Graylog interface: {}", e));
                        socket.flush().unwrap_or_else(
                            |e| warn!("Could not send log to Graylog interface: {}", e));
                    }
                    Err(e) => warn!("Could not serialize a log in Graylog interface: {}.", e)
                }
            }
        }
    }
}


pub fn add_timestamp_field(log: &mut ArbitraryJson) -> Result<(), std::io::Error> {

    let time_value = if let Some(i) = log.get("CreationTime") {
        i
    } else {
        return Err(std::io::Error::new(
            ErrorKind::NotFound, "Expected CreationTime field".to_string()))
    };

    let time_string = if let Some(i) = time_value.as_str() {
        i
    } else {
        return Err(std::io::Error::new(
            ErrorKind::NotFound, "Could not convert timestamp field to string".to_string()))

    };

    let time = if let Ok(i) =
            NaiveDateTime::parse_from_str(time_string, "%Y-%m-%dT%H:%M:%S") {
        i
    } else {
        return Err(std::io::Error::new(
            ErrorKind::NotFound, "Could parse time of log".to_string()))
    };

    let time_utc = DateTime::<Utc>::from_naive_utc_and_offset(time, Utc);
    let mut time_stamp = time_utc.format("%Y-%m-%d %H:%M:%S.%f").to_string();
    time_stamp = time_stamp[..time_stamp.len() - 6].to_string();
    log.insert("timestamp".to_string(), Value::String(time_stamp));
    Ok(())
}
