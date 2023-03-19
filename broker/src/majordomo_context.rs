use crate::data_structures::{Identity, WorkerInteractionType};
use domolib::errors::RustydomoError;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Display, Error, Formatter};
use std::rc::Rc;

const EXPIRATION_TIME: std::time::Duration = std::time::Duration::from_secs(1);

struct ServiceInfo {
    service_name: String,
    identity: Identity,
    expiration_date: std::time::Instant,
}

impl Display for ServiceInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{} : {:?}", &self.service_name, self.identity.value)?;
        Ok(())
    }
}

///
/// Base context used to carry all useful informations for proper broker interactiions management
///
pub struct MajordomoContext {
    /// List of all registered workers jnown to the system
    registered_workers: VecDeque<Rc<RefCell<ServiceInfo>>>,
    /// List of workers registered by service name
    services: HashMap<String, Vec<Rc<RefCell<ServiceInfo>>>>,
}

impl MajordomoContext {
    pub fn new() -> Self {
        MajordomoContext {
            registered_workers: VecDeque::new(),
            services: HashMap::new(),
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
            .any(|entry| entry.borrow().service_name == service_name)
    }

    ///
    /// Registeres given task and sent its associated payload to next available worker
    ///
    /// Note: if multiple workers are registerd to handle, the workers are selected in a round
    /// robin fashion to ensure proper equity between workers
    ///
    /// # Arguments
    ///
    /// * `workers_connection` - connection used to send requests to all registered workers
    ///
    /// * `service_name` - Name of the service for which task has to be sent
    ///
    /// * `payload` - Actual payload associated to the service call
    pub fn send_task_to_worker(
        self: &mut Self,
        workers_connection: &zmq::Socket,
        service_name: String,
        payload: Vec<Vec<u8>>,
    ) -> Result<(), RustydomoError> {
        if self.can_handle_service(&service_name) {
            log::info!("Queuing task '{}' with payload length being", service_name,);
            self.process_tasks(&workers_connection, service_name, payload)?;
        } else {
            return Err(RustydomoError::ServiceNotAvailable(service_name.into()));
        }

        Ok(())
    }

    ///
    /// Registers a new worker so that it can be used to dispatch tasks
    ///
    /// # Arguments
    ///
    /// * `identity` - actual identity associated to this new worker (zmq identity)
    /// * `service_name` - Service handled by the given worker
    ///
    pub fn register_worker(
        self: &mut Self,
        identity: &[u8],
        service_name: &str,
    ) -> Result<(), RustydomoError> {
        let value_to_insert = Rc::new(RefCell::new(ServiceInfo {
            service_name: service_name.into(),
            identity: Identity::try_from(identity).unwrap(),
            expiration_date: std::time::Instant::now() + (EXPIRATION_TIME * 4),
        }));
        self.registered_workers.push_front(value_to_insert.clone());
        log::info!(
            "Registered new worker for service '{}'. Identity : '{:?}'",
            service_name,
            identity
        );

        // create entry in the map if it does not exist
        if !self.services.contains_key(service_name) {
            self.services.insert(service_name.to_string(), Vec::new());
        }
        // finally register the worker
        self.services
            .get_mut(service_name.into())
            .unwrap()
            .push(value_to_insert);

        Ok(())
    }

    ///
    /// Updates given worker (specified by its identity) to update its expiration date
    /// appropriately
    ///
    /// # Arguments
    ///
    /// * `identity` - actual identity associated to the worker to be updated
    ///
    pub fn refresh_expiration_time(self: &mut Self, identity: &[u8]) -> Result<(), RustydomoError> {
        let mut idx = 0;
        let searched_identity: Identity = Identity::try_from(identity)?;
        loop {
            match self.registered_workers.get(idx) {
                Some(cur_id) => {
                    if cur_id.borrow().identity == searched_identity {
                        // found entry, no refresh its expiration time
                        let cur_entry = self.registered_workers.remove(idx).unwrap();
                        // update with new expiration date before reinserting it
                        cur_entry.borrow_mut().expiration_date =
                            std::time::Instant::now() + EXPIRATION_TIME;
                        self.registered_workers.push_front(cur_entry);
                        break;
                    } else {
                        idx += 1;
                        // increment index as it is not the right entry
                    }
                }
                None => break, // leave loop at this point, no more data to fetch
            }
        }
        Ok(())
    }

    pub fn remove_worker(self: &mut Self, identity: &[u8]) -> Result<(), RustydomoError> {
        let searched_identity: Identity = Identity::try_from(identity)?;
        log::debug!("Removing worker (cause : DISCONNECT received)");

        let found_worker = self
            .registered_workers
            .iter()
            .position(|entry| entry.borrow().identity == searched_identity);

        if let Some(pos) = found_worker {
            let removed_value = self.registered_workers.remove(pos).unwrap();
            let service_workers = self
                .services
                .get_mut(&removed_value.borrow().service_name)
                .unwrap();
            service_workers.retain(|entry| !Rc::ptr_eq(&entry, &removed_value));
            log::debug!("Worker removed");
        } else {
            return Err(RustydomoError::ServiceNotAvailable(format!(
                "Identity : {:?}",
                searched_identity.value
            )));
        }

        Ok(())
    }
    ///
    /// Send all requested tasks to all available workers
    /// This apply a simple round robin mechnism to balance work between multiple workers
    pub fn process_tasks(
        self: &mut Self,
        workers_connection: &zmq::Socket,
        target_service: String,
        payload: Vec<Vec<u8>>,
    ) -> Result<(), RustydomoError> {
        //             let worker = self
        //                 .registered_workers
        //                 .iter_mut()
        //                 .find(|entry| entry.borrow().service_name == task.target_service);

        let avail_workers = self.services.get_mut(&target_service).unwrap();
        let worker = avail_workers.first();

        match worker {
            Some(entry) => {
                log::info!(
                    "Sending task '{}' on worker '{}'",
                    target_service,
                    entry.borrow()
                );
                //send identity first, the the rest of the payload
                workers_connection
                    .send::<Vec<u8>>(entry.borrow().identity.clone().into(), zmq::SNDMORE)
                    .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;

                workers_connection
                    .send::<&[u8]>("MDPW02".as_bytes(), zmq::SNDMORE)
                    .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;

                workers_connection
                    .send_multipart(payload.iter(), 0)
                    .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;

                // rotate worker in a round robin fashion afterwards
                if avail_workers.len() > 1 {
                    avail_workers.rotate_left(1);
                }
            }
            None => log::debug!(
                "Task for service '{}' not handled this turn",
                target_service
            ),
        }

        Ok(())
    }

    pub fn check_expired_workers(self: &mut Self) {
        // just fetch from start until we reach a point where we are not considered expired (they
        // are already sorted from the older to the newest
        let ref_time = std::time::Instant::now();

        loop {
            if let Some(curentry) = self.registered_workers.back() {
                if curentry.borrow().expiration_date < ref_time {
                    let associated_node = self
                        .registered_workers
                        .remove(self.registered_workers.len() - 1)
                        .unwrap();
                    // remove also the entry from services worker list
                    let local_workers = self
                        .services
                        .get_mut(&associated_node.borrow().service_name)
                        .unwrap();
                    let old_len = local_workers.len();

                    local_workers.retain(|entry| !Rc::ptr_eq(&entry, &associated_node));
                    log::debug!(
                        "Service workers removed : {}",
                        old_len - local_workers.len()
                    );
                } else {
                    // assume all next elements are also ok in terms of expiration date
                    break;
                }
            } else {
                break;
            }
        }
    }

    pub fn send_heartbeat(self: &Self, worker_sock: &zmq::Socket) -> Result<(), RustydomoError> {
        let hearbeat_command: Vec<u8> = vec![WorkerInteractionType::Heartbeat as u8];

        for worker in self.registered_workers.iter() {
            worker_sock
                .send::<Vec<u8>>(worker.borrow().identity.clone().into(), zmq::SNDMORE)
                .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
            // send appropriate header
            worker_sock
                .send("MDPW02".as_bytes(), zmq::SNDMORE)
                .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
            // send the final part of the command (the actual HEARTBEAT payload)
            worker_sock
                .send::<&[u8]>(&hearbeat_command, 0)
                .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
        }
        Ok(())
    }
}
