use crate::data_structures::ConnectionData;
use std::collections::HashMap;

use crate::errors::RustydomoError;

struct ServiceInfo {
    service_connection: ConnectionData,
    busy: bool,
}

pub struct MajordomoContext {
    registered_services: HashMap<String, Vec<ServiceInfo>>,
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

    pub fn queue_task(
        self: &mut Self,
        service_name: &str,
        payload: Vec<Vec<u8>>,
    ) -> Result<(), RustydomoError> {
        if let Some(entry) = self.registered_services.get_mut(service_name) {
            MajordomoContext::handle_task(&mut *entry, payload)?
        } else {
            return Err(RustydomoError::ServiceNotAvailable(service_name.into()));
        }

        Ok(())
    }

    fn handle_task(
        avail_services: &mut Vec<ServiceInfo>,
        payload: Vec<Vec<u8>>,
    ) -> Result<(), RustydomoError> {
        match avail_services.into_iter().find(|curentry| !curentry.busy) {
            Some(entry) => {
                entry.busy = true;
                entry
                    .service_connection
                    .connection
                    .send_multipart(payload, 0) // found available connection, send data to it
                    .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
            }
            None => todo!(),
        }
        Ok(())
    }
}
