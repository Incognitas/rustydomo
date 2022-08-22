#[derive(Debug, Clone)]
pub enum RustydomoError {
    SocketCreationError(String),
    SocketBindingError(String),
    MonitorCreationError(String),
    CommunicationError(String),
    ConversionError,
    Unknown,
}
