use crate::data_structures::ConnectionData;
use std::collections::HashMap;
use std::fmt::{Display, Error, Formatter};

use crate::errors::RustydomoError;

const EXPIRATION_TIME: std::time::Duration = std::time::Duration::from_micros(500);

struct ServiceInfo {
    service_name: String,
    identity: Vec<u8>,
    expiration_date: std::time::Instant,
}

impl Display for ServiceInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        let mut array_to_parse: [u8; 4] = Default::default();
        array_to_parse.copy_from_slice(&self.identity[..4]);

        write!(f, "{:08X}", u32::from_ne_bytes(array_to_parse))?;
        Ok(())
    }
}

struct Task {
    target_service: String,
    payload: Vec<Vec<u8>>,
}

pub struct MajordomoContext {
    registered_workers: Vec<ServiceInfo>,
    tasks_list: Vec<Task>,
}

impl MajordomoContext {
    pub fn new() -> Self {
        MajordomoContext {
            registered_workers: Vec::new(),
            tasks_list: Vec::new(),
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
        self.registered_workers
            .iter()
            .any(|entry| entry.service_name == service_name)
    }

    pub fn queue_task(
        self: &mut Self,
        service_name: &str,
        payload: Vec<Vec<u8>>,
    ) -> Result<(), RustydomoError> {
        if self.can_handle_service(service_name) {
            self.tasks_list.push(Task {
                target_service: service_name.into(),
                payload,
            });
        } else {
            return Err(RustydomoError::ServiceNotAvailable(service_name.into()));
        }

        Ok(())
    }

    pub fn register_worker(
        self: &mut Self,
        identity: Vec<u8>,
        service_name: &str,
    ) -> Result<(), RustydomoError> {
        log::info!("Registering new worker for service '{}'", service_name);

        self.registered_workers.push(ServiceInfo {
            service_name: service_name.into(),
            identity,
            expiration_date: std::time::Instant::now() + EXPIRATION_TIME,
        });

        Ok(())
    }

    pub fn process_tasks(
        self: &mut Self,
        workers_connection: &ConnectionData,
    ) -> Result<(), RustydomoError> {
        if self.tasks_list.is_empty() {
            return Ok(());
        }

        let mut tasks_handled = Vec::new();

        for i in 0..self.tasks_list.len() {
            let task: &Task = self.tasks_list.get(i).unwrap();
            let worker = self
                .registered_workers
                .iter_mut()
                .find(|entry| entry.service_name == task.target_service);

            match worker {
                Some(entry) => {
                    log::info!(
                        "Sending task '{}' on worker '{}'",
                        task.target_service,
                        *entry
                    );

                    workers_connection
                        .connection
                        .send_multipart(task.payload.iter(), 0)
                        .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
                    tasks_handled.push(i);
                }
                None => log::debug!("Task for service {} not handled this turn", "plop"),
            }
            if self.registered_workers.len() > 1 {
                // rotate the list of workers to put the eventual next worker in position to
                // be used next
                self.registered_workers.rotate_left(1);
            }
        }
        if !tasks_handled.is_empty() {
            // cleanup all indexes of tasks that have been handled up to now
            tasks_handled.iter_mut().rev().for_each(|idx| {
                self.tasks_list.swap_remove(*idx);
            });
        }
        Ok(())
    }
}
