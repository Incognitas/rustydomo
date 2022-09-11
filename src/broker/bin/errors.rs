use core::fmt;

#[derive(Debug, Clone)]
pub enum RustydomoError {
    SocketCreationError(String),
    SocketBindingError(String),
    MonitorCreationError(String),
    CommunicationError(String),
    UnrecognizedCommandType(u8),
    ServiceNotAvailable(String),
    ConversionError(String),
    Unknown(String),
}

impl fmt::Display for RustydomoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SocketCreationError(value) => {
                write!(f, "Error during socket creation : {}", value)
            }
            Self::SocketBindingError(value) => {
                write!(f, "Error during socket binding : {}", value)
            }
            Self::MonitorCreationError(value) => {
                write!(f, "Failed to create monitor for connection : {}", value)
            }
            Self::CommunicationError(value) => {
                write!(f, "Error during communication : {}", value)
            }
            Self::UnrecognizedCommandType(value) => {
                write!(f, "Unrecognized command : {}", value)
            }

            Self::ServiceNotAvailable(value) => {
                write!(f, "Requested service does not exist : '{}'", value)
            }
            Self::ConversionError(value) => {
                write!(f, "Failed to during value conversion: {}", value)
            }
            Self::Unknown(value) => {
                write!(f, "Unknown error occured : '{}'", value)
            }
        }
    }
}
