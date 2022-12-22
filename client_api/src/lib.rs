mod api;

use api::{ClientError, ClientInterface, Request};
use zmq::{Context, SocketType};

struct Client {
    ctx: zmq::Context,
    sock: zmq::Socket,
}

impl Client {
    fn new(connection_string: &str) -> Result<Self, ClientError> {
        let ctx = zmq::Context::new();
        let result = Client {
            sock: ctx.socket(SocketType::DEALER).unwrap(),
            ctx,
        };
        result.connect(connection_string)?;
        return Ok(result);
    }
}

impl ClientInterface for Client {
    fn disconnect(&self) -> bool {
        false
    }

    fn connect(&self, connection_string: &str) -> Result<(), api::ClientError> {
        self.sock
            .connect(connection_string)
            .map_err(|err| api::ClientError::CommunicationError(err.to_string()))?;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        todo!()
    }

    fn send_request(
        &self,
        service_name: &str,
        payload: &Vec<Vec<u8>>,
    ) -> Result<Box<dyn Request>, api::ClientError> {
        todo!()
    }
}
