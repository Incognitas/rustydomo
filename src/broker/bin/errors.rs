#[derive(Debug, Clone)]
pub enum RustydomoError {
    SocketCreationError(String),
    SocketBindingError(String),
    MonitorCreationError(String),
    CommunicationError(String),
    UnrecognizedCommandType(u8),
    ServiceNotAvailable(String),
    ConversionError,
    Unknown(String),
}
