use zmq::SocketType;

const EXPECTED_CLIENT_VERSION_HEADER: &str = "MDPC02";

#[derive(Debug)]
pub enum ClientError {
    InitializationError(String),
    CommunicationError(String),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClientRequestState {
    PARTIAL = 2,
    FINAL = 3,
}

pub struct RequestResult {
    pub state: ClientRequestState,
    pub payload: Vec<Vec<u8>>,
}

pub struct Client {
    client_connection: Option<zmq::Socket>,
}

pub struct ClientRequest<'a> {
    client: &'a Client,
    request_ongoing: bool,
}

impl Client {
    pub fn new(connection_string: &str) -> Result<Self, ClientError> {
        let ctx = zmq::Context::new();
        let result = Client {
            client_connection: Some(ctx.socket(SocketType::DEALER).unwrap()),
        };

        match &result.client_connection {
            Some(connection) => {
                connection.connect(&connection_string).unwrap();
            }
            _ => {
                log::error!("Failed to create connection to the broker");
            }
        }

        return Ok(result);
    }
}

impl Client {
    pub fn send_request(
        &self,
        service_name: &str,
        payload: &Vec<Vec<u8>>,
    ) -> Option<ClientRequest> {
        let result = ClientRequest {
            client: &self,
            request_ongoing: true,
        };
        if let Some(connection) = &self.client_connection {
            let request_type: [u8; 1] = [1];
            connection.send("MDPC02".as_bytes(), zmq::SNDMORE).unwrap();
            connection
                .send(request_type.as_slice(), zmq::SNDMORE)
                .unwrap();
            connection.send(service_name, zmq::SNDMORE).unwrap();
            connection.send_multipart(payload, 0).unwrap();
            return Some(result);
        }

        None
    }
}
fn receive_and_check_broker_response(sock: &zmq::Socket) -> Option<RequestResult> {
    // we assume here that we have data waiting for us
    let mut msg = zmq::Message::new();

    sock.recv(&mut msg, 0)
        .expect("Failed to receive response header");

    // ensure that we received the MDPC02 client header

    if let Some(obtained) = msg.as_str() {
        match obtained {
            EXPECTED_CLIENT_VERSION_HEADER => sock.recv(&mut msg, 0).unwrap(),
            _ => {
                log::error!("Unrecognized client version received");
                return None;
            }
        }
    }

    // now check the type of command (PARTIAL/FINAL)
    // if they are found, just push the payload by receiving the rest of the frames
    match msg.into_iter().nth(0) {
        Some(&x) if x == ClientRequestState::PARTIAL as u8 => {
            return Some(RequestResult {
                state: ClientRequestState::PARTIAL,
                payload: sock.recv_multipart(0).unwrap(),
            })
        }
        Some(&x) if x == ClientRequestState::FINAL as u8 => {
            return Some(RequestResult {
                state: ClientRequestState::FINAL,
                payload: sock.recv_multipart(0).unwrap(),
            })
        }
        Some(state) => {
            log::error!("Unrecognized state : {}", state);
            return None;
        }
        _ => return None,
    };
}

impl<'a> Iterator for ClientRequest<'a> {
    type Item = RequestResult;

    fn next(&mut self) -> Option<Self::Item> {
        // no need to poll if request is finished
        if !self.request_ongoing {
            return None;
        };

        match &self.client.client_connection {
            Some(connection) => loop {
                let mut poll_list = [connection.as_poll_item(zmq::POLLIN)];

                // time to poll events for all sockets
                match zmq::poll(&mut poll_list, 100) {
                    Ok(nbitemspolled) => {
                        if nbitemspolled > 0 {
                            // we only have one socket to monitor, no need to over engineer this
                            let returned_state = receive_and_check_broker_response(&connection);
                            match &returned_state {
                                Some(entry) => {
                                    // update request status based on returned state
                                    if entry.state == ClientRequestState::FINAL {
                                        // if it is the final answer, we consider this step the final
                                        // one
                                        log::debug!("End of the current loop");
                                        self.request_ongoing = false;
                                    }
                                }
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
