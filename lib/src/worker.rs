use crate::errors::WorkerError;
use zmq::SocketType;

const EXPECTED_WORKER_VERSION_HEADER: &str = "MDPW02";

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerRequestState {
    READY = 1,
    REQUEST = 2,
    PARTIAL = 3,
    FINAL = 4,
    HEARTBEAT = 5,
    DISCONNECT = 6,
}

pub struct WorkerRequestResult {
    pub state: WorkerRequestState,
    pub payload: Vec<Vec<u8>>,
}

pub struct Worker {
    worker_connection: Option<zmq::Socket>,
    task_handled: String,
}

pub struct WorkerRequest<'a> {
    worker: &'a Worker,
    request_ongoing: bool,
}

impl Worker {
    pub fn new(task_name: String, broker_connection_string: &str) -> Result<Self, WorkerError> {
        let ctx = zmq::Context::new();
        let result = Worker {
            worker_connection: Some(ctx.socket(SocketType::DEALER).unwrap()),
            task_handled: task_name,
        };

        match &result.worker_connection {
            Some(connection) => {
                connection.connect(&broker_connection_string).unwrap();
            }
            _ => {
                log::error!("Failed to create connection to the broker");
            }
        }

        return Ok(result);
    }

    pub fn register_to_broker(&self) -> Result<(), WorkerError> {
        if let Some(connection) = &self.worker_connection {
            let request_type: [u8; 1] = [WorkerRequestState::READY as u8];
            connection
                .send(EXPECTED_WORKER_VERSION_HEADER, zmq::SNDMORE)
                .unwrap();
            connection
                .send(request_type.as_slice(), zmq::SNDMORE)
                .unwrap();
            connection.send(self.task_handled.as_str(), 0).unwrap();

            log::info!("Registered worker for task '{}'", self.task_handled);

            Ok(())
        } else {
            Err(WorkerError::InitializationError(
                "No connection created".into(),
            ))
        }
    }

    pub fn send_heartbeat(&self) -> Result<(), WorkerError> {
        if let Some(connection) = &self.worker_connection {
            let request_type: [u8; 1] = [WorkerRequestState::HEARTBEAT as u8];
            connection
                .send(EXPECTED_WORKER_VERSION_HEADER, zmq::SNDMORE)
                .unwrap();
            connection.send(request_type.as_slice(), 0).unwrap();

            log::debug!("Worker sending hearbeat");

            Ok(())
        } else {
            Err(WorkerError::InitializationError(
                "No connection created".into(),
            ))
        }
    }

    pub fn disconnect_from_broker(&self) -> Result<(), WorkerError> {
        if let Some(connection) = &self.worker_connection {
            let request_type: [u8; 1] = [WorkerRequestState::DISCONNECT as u8];
            connection
                .send(EXPECTED_WORKER_VERSION_HEADER, zmq::SNDMORE)
                .unwrap();
            connection.send(request_type.as_slice(), 0).unwrap();

            log::debug!("Worker sending hearbeat");

            Ok(())
        } else {
            Err(WorkerError::InitializationError(
                "No connection created".into(),
            ))
        }
    }
}

fn receive_and_handle_broker_request(sock: &zmq::Socket) -> Option<WorkerRequestResult> {
    // we assume here that we have data waiting for us
    let mut msg = zmq::Message::new();

    sock.recv(&mut msg, 0)
        .expect("Failed to receive response header");

    // ensure that we received the MDPW02 worker header

    if let Some(obtained) = msg.as_str() {
        match obtained {
            EXPECTED_WORKER_VERSION_HEADER => sock.recv(&mut msg, 0).unwrap(),
            _ => {
                log::error!("Unrecognized worker version received");
                return None;
            }
        }
    }

    // now check the type of command (PARTIAL/FINAL)
    // if they are found, just push the payload by receiving the rest of the frames
    match msg.into_iter().nth(0) {
        Some(&x) if x == WorkerRequestState::PARTIAL as u8 => {
            return Some(WorkerRequestResult {
                state: WorkerRequestState::PARTIAL,
                payload: sock.recv_multipart(0).unwrap(),
            })
        }
        Some(&x) if x == WorkerRequestState::FINAL as u8 => {
            return Some(WorkerRequestResult {
                state: WorkerRequestState::FINAL,
                payload: sock.recv_multipart(0).unwrap(),
            })
        }
        Some(&x) if x == WorkerRequestState::HEARTBEAT as u8 => {
            todo!("Handle heartbeat sent by the broker")
        }
        Some(state) => {
            log::error!("Unhandled state : {}", state);
            return None;
        }
        _ => return None,
    };
}

impl<'a> Iterator for WorkerRequest<'a> {
    type Item = WorkerRequestResult;

    fn next(&mut self) -> Option<Self::Item> {
        // no need to poll if request is finished
        if !self.request_ongoing {
            return None;
        };

        match &self.worker.worker_connection {
            Some(connection) => loop {
                let mut poll_list = [connection.as_poll_item(zmq::POLLIN)];

                // time to poll events for all sockets
                match zmq::poll(&mut poll_list, 100) {
                    Ok(nbitemspolled) => {
                        if nbitemspolled > 0 {
                            // we only have one socket to monitor, no need to over engineer this
                            let returned_state = receive_and_handle_broker_request(&connection);
                            match &returned_state {
                                Some(entry) => match entry.state {
                                    // update request status based on returned state
                                    WorkerRequestState::FINAL => {
                                        // if it is the final answer, we consider this step the final
                                        // one and we will end iteration
                                        log::debug!("End of the current loop");
                                        self.request_ongoing = false;
                                    }
                                    WorkerRequestState::HEARTBEAT => {
                                        // TODO: handle hearbeats from broker (refresh timestamps)
                                        continue;
                                    }
                                    WorkerRequestState::PARTIAL => {
                                        // nothing to do, just let it through
                                    }
                                    _ => {
                                        log::error!("Unhandled state received : {:?}", entry.state);
                                        continue;
                                    }
                                },
                                None => (),
                            }
                            return returned_state;
                        }

                        // request is actually ongoing, just continue looping
                        if self.request_ongoing {
                            // nothing to do, just continue looping
                            continue;
                        }
                    }
                    Err(_) => (),
                }
            },
            None => (),
        }

        return None;
    }
}
