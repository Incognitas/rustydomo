use crate::data_structures::{ConnectionData, Identity};
use crate::errors::RustydomoError;
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
        write!(f, "{} : {:08X}", &self.service_name, self.identity.value)?;
        Ok(())
    }
}

struct Task {
    target_service: String,
    payload: Vec<Vec<u8>>,
}

pub struct MajordomoContext {
    registered_workers: VecDeque<Rc<RefCell<ServiceInfo>>>,
    tasks_list: Vec<Task>,
    services: HashMap<String, Vec<Rc<RefCell<ServiceInfo>>>>,
}

impl MajordomoContext {
    pub fn new() -> Self {
        MajordomoContext {
            registered_workers: VecDeque::new(),
            tasks_list: Vec::new(),
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

    pub fn queue_task(
        self: &mut Self,
        service_name: String,
        payload: Vec<Vec<u8>>,
    ) -> Result<(), RustydomoError> {
        if self.can_handle_service(&service_name) {
            log::info!(
                "Queuing task '{}' with payload length being {}...",
                service_name,
                payload.len()
            );
            self.tasks_list.push(Task {
                target_service: service_name.into(),
                payload,
            });
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
            expiration_date: std::time::Instant::now() + EXPIRATION_TIME,
        }));
        self.registered_workers.push_front(value_to_insert.clone());
        log::info!(
            "Registered new worker for service '{}'. Identity : '{}'",
            service_name,
            value_to_insert.borrow()
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

    ///
    /// Send all requested tasks to all available workers
    /// This apply a simple round robin mechnism to balance work between multiple workers
    pub fn process_tasks(
        self: &mut Self,
        workers_connection: &ConnectionData,
    ) -> Result<(), RustydomoError> {
        if self.tasks_list.is_empty() {
            return Ok(());
        }

        for i in 0..self.tasks_list.len() {
            let task: &Task = self.tasks_list.get(i).unwrap();
            //             let worker = self
            //                 .registered_workers
            //                 .iter_mut()
            //                 .find(|entry| entry.borrow().service_name == task.target_service);

            let avail_workers = self.services.get_mut(&task.target_service).unwrap();
            let worker = avail_workers.first();

            match worker {
                Some(entry) => {
                    log::info!(
                        "Sending task '{}' on worker '{}'",
                        task.target_service,
                        entry.borrow()
                    );
                    //send identity first, the the rest of the payload
                    workers_connection
                        .connection
                        .send::<Vec<u8>>(entry.borrow().identity.into(), zmq::SNDMORE)
                        .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;

                    workers_connection
                        .connection
                        .send_multipart(task.payload.iter(), 0)
                        .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;

                    // rotate worker in a round robin fashion afterwards
                    if avail_workers.len() > 1 {
                        avail_workers.rotate_left(1);
                    }
                }
                None => log::debug!(
                    "Task for service '{}' not handled this turn",
                    &task.target_service
                ),
            }
        }

        // clear all content of tasks list
        self.tasks_list.clear();
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
        // let nb_known_workers = self.registered_workers.len();
        // self.registered_workers
        //     .retain(|entry| entry.borrow().expiration_date >= ref_time);
        // let nb_workers_removed = nb_known_workers - self.registered_workers.len();

        // if nb_workers_removed > 0 {
        //     log::debug!("Workers removed after expiration : {}", nb_workers_removed);
        // }
    }
}
