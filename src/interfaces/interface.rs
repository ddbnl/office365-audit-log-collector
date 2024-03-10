use crate::data_structures::Caches;

pub trait Interface {
    fn send_logs(&mut self, logs: Caches);
}