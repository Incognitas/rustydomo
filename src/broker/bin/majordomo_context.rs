use std::collections::HashMap;

pub struct MajordomoContext {
    registered_services: HashMap<String, Vec<zmq::Socket>>,
}

impl MajordomoContext {
    pub fn new() -> Self {
        MajordomoContext {
            registered_services: HashMap::new(),
        }
    }

    /// Indicates whether or not the given service name can be handled by the broker currently
    ///
    /// It just checks whether a service with appropriate name has been registered previously
    ///
    /// # Arguments
    ///
    /// * `service_name` - service name to check
    ///
    pub fn can_handle_service(self: &Self, service_name: &str) -> bool {
        self.registered_services.contains_key(service_name)
    }
}
