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
