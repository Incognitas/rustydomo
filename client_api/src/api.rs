pub enum ClientError {
    CommunicationError(String),
}

pub enum RequestState {
    PARTIAL,
    FINAL,
}

pub struct RequestResult {
    state: RequestState,
    payload: Vec<Vec<u8>>,
}

pub trait Request {
    fn is_over(&self) -> bool;
    fn read_status(&self) -> Result<RequestResult, ClientError>;
}

pub trait ClientInterface {
    fn connect(&self, connection_string: &str) -> Result<(), ClientError>;

    fn is_connected(&self) -> bool;

    fn send_request(
        &self,
        service_name: &str,
        payload: &Vec<Vec<u8>>,
    ) -> Result<Box<dyn Request>, ClientError>;

    fn disconnect(&self) -> bool;
}
