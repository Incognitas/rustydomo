use crate::api::{ClientError, ClientInterface, Request};
use crate::structures::MessageHelper;
use std::sync::Arc;
use std::sync::{atomic::AtomicU64, Mutex};
use std::thread;
use zmq::{SocketEvent, SocketType};

static GLOBAL_ID_COUNT: AtomicU64 = AtomicU64::new(1); // let's just start at 1 for a change

struct Client {
    connection_address: String,
    sock: zmq::Socket,
    sock_monitor: zmq::Socket,
    connection_status: zmq::SocketEvent,
}

impl Client {
    fn new(connection_string: &str) -> Result<Self, ClientError> {
        let ctx = zmq::Context::new();
        // create the address that will be used to connect to the monitor
        // this allows the creation of multiple clients in same application
        let monitor_address = format!(
            "inproc://_client_monitor_{}",
            GLOBAL_ID_COUNT.load(std::sync::atomic::Ordering::SeqCst)
        );

        let client_socket = ctx
            .socket(SocketType::DEALER)
            .map_err(|err| ClientError::InitializationError(err.to_string()))?;

        // then create the monitor around this connection
        client_socket
            .monitor(
                monitor_address.as_str(),
                SocketEvent::DISCONNECTED as i32 | SocketEvent::ACCEPTED as i32,
            )
            .map_err(|err| ClientError::CommunicationError(err.to_string()))?;

        // create monitor connection for listening to client socket events
        let sock_monitor = ctx.socket(zmq::PAIR).unwrap();
        // connect the monitor socket already even if the monitored connection is not activated yet
        sock_monitor
            .connect(&monitor_address.as_str())
            .map_err(|err| ClientError::InitializationError(err.to_string()))?;

        // finally
        let result = Client {
            connection_address: connection_string.to_owned(),
            sock: client_socket,
            sock_monitor,
            connection_status: zmq::SocketEvent::DISCONNECTED,
        };
        // finally activate the connection between the instance and the broker
        result.connect()?;

        return Ok(result);
    }
}

impl ClientInterface for Client {
    fn connect(&self) -> Result<(), ClientError> {
        // create the connection first....
        self.sock
            .connect(&self.connection_address)
            .map_err(|err| ClientError::CommunicationError(err.to_string()))?;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connection_status == zmq::SocketEvent::CONNECTED
    }

    fn send_request(
        &self,
        service_name: &str,
        payload: &Vec<Vec<u8>>,
    ) -> Result<Box<dyn Request>, ClientError> {
        todo!()
    }

    fn disconnect(&self) -> bool {
        if let Ok(_) = self.sock.disconnect(&self.connection_address) {
            true
        } else {
            false
        }
    }
}

impl Client {
    fn start_poller_thread(this: Arc<Mutex<Self>>) {
        // TODO:
        // THis background thread will:
        // - create the connection and the monitor
        // - interact with the cleint ionstance through inproc
        //
        thread::spawn(move || loop {
            let mut locked_self = this.lock().expect("Mutex poisoned");

            let mut poll_item_list = [locked_self.sock_monitor.as_poll_item(zmq::POLLIN)];

            match zmq::poll(&mut poll_item_list, 200) {
                Ok(_) => {
                    // poll events from Monitor socket at this point, this is the onl
                    // one we are listening to
                    let mut msg: zmq::Message = zmq::Message::new();
                    locked_self
                        .sock_monitor
                        .recv(&mut msg, 0)
                        .expect("Internal socket error");

                    let helper = MessageHelper { m: msg };

                    // return the obtained value
                    let event_id: u16 = helper.try_into().unwrap();

                    match event_id {
                        x if x == zmq::SocketEvent::CONNECTED as u16 => {
                            locked_self.connection_status = zmq::SocketEvent::CONNECTED;
                        }
                        x if x == zmq::SocketEvent::DISCONNECTED as u16 => {
                            locked_self.connection_status = zmq::SocketEvent::DISCONNECTED;
                        }
                        _ => {
                            log::error!("Unrecognized event : {}", event_id)
                        }
                    }
                }
                Err(err) => log::error!("Failed to poll data: {}", err.to_string()),
            };
        });
    }
}
