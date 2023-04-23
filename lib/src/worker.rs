use crate::errors::WorkerError;
use std::time::Duration;
use std::time::Instant;
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
    pub payload: Option<Vec<Vec<u8>>>,
}

pub type TaskHandlerFunction = fn(&Worker, &Option<Vec<Vec<u8>>>) -> ();

pub struct Worker {
    worker_connection: Option<zmq::Socket>,
    task_handled: String,
    task_handler: TaskHandlerFunction,
    connected: bool,
    last_broker_keepalive_time: Instant,
}

impl Worker {
    pub fn new(
        task_name: String,
        broker_connection_string: &str,
        handler: TaskHandlerFunction,
    ) -> Result<Self, WorkerError> {
        let ctx = zmq::Context::new();
        let mut result = Worker {
            worker_connection: Some(ctx.socket(SocketType::DEALER).unwrap()),
            task_handled: task_name,
            task_handler: handler,
            connected: false,
            last_broker_keepalive_time: Instant::now(),
        };

        match &result.worker_connection {
            Some(connection) => {
                connection.connect(&broker_connection_string).unwrap();
                result.connected = true;
            }
            _ => {
                log::error!("Failed to create connection to the broker");
            }
        }

        return Ok(result);
    }

    pub fn register_to_broker(&mut self) -> Result<(), WorkerError> {
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
            self.connected = true;

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

    pub fn check_broker_connection_expired(&self) -> bool {
        return (Instant::now() - self.last_broker_keepalive_time) > Duration::from_secs(1);
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
        Some(&x) if x == WorkerRequestState::REQUEST as u8 => {
            return Some(WorkerRequestResult {
                state: WorkerRequestState::REQUEST,
                payload: Some(sock.recv_multipart(0).unwrap()),
            })
        }
        Some(&x) if x == WorkerRequestState::HEARTBEAT as u8 => {
            return Some(WorkerRequestResult {
                state: WorkerRequestState::HEARTBEAT,
                payload: None,
            })
        }
        Some(&x) if x == WorkerRequestState::DISCONNECT as u8 => {
            return Some(WorkerRequestResult {
                state: WorkerRequestState::DISCONNECT,
                payload: None,
            })
        }
        Some(state) => {
            log::error!("Unhandled state : {}", state);
            return None;
        }
        _ => return None,
    };
}

impl Worker {
    pub fn process(&mut self) {
        match &self.worker_connection {
            Some(connection) => {
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
                                    WorkerRequestState::REQUEST => {
                                        if self.connected {
                                            (self.task_handler)(&self, &entry.payload);
                                        } else {
                                            log::error!("Received request to execute task '{}' although the worker is not READY.", self.task_handled);
                                        }
                                    }
                                    WorkerRequestState::DISCONNECT => {
                                        // mark as not connected to ensure the READY signal will be
                                        // sent next time a command has to be sent
                                        self.connected = false;
                                    }
                                    WorkerRequestState::HEARTBEAT => {
                                        self.last_broker_keepalive_time = Instant::now();
                                    }
                                    _ => {
                                        log::error!("Unhandled state received : {:?}", entry.state);
                                    }
                                },
                                None => (),
                            }
                        }
                    }
                    Err(_) => (),
                }
            }
            None => (),
        }
    }
}
