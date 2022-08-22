use crate::errors::RustydomoError;
use zmq::{Message, Socket};

pub enum SocketType {
    ClientSocket = 0,
    ClientMonitorSocket,
    ServiceSocket,
    ServiceMonitorSocket,
}

impl TryFrom<usize> for SocketType {
    type Error = RustydomoError;

    fn try_from(val: usize) -> Result<Self, Self::Error> {
        match val {
            x if x == SocketType::ClientSocket as usize => Ok(SocketType::ClientSocket),
            x if x == SocketType::ClientMonitorSocket as usize => {
                Ok(SocketType::ClientMonitorSocket)
            }
            x if x == SocketType::ServiceSocket as usize => Ok(SocketType::ServiceSocket),
            x if x == SocketType::ServiceMonitorSocket as usize => {
                Ok(SocketType::ServiceMonitorSocket)
            }
            _ => Err(RustydomoError::Unknown),
        }
    }
}

pub struct ConnectionData {
    pub connection: Socket,
    pub monitor_connection: Socket,
}

pub struct MessageHelper {
    pub m: Message,
}

impl TryInto<u16> for MessageHelper {
    type Error = RustydomoError;

    fn try_into(self) -> Result<u16, Self::Error> {
        if let Some(content) = self.m.as_str() {
            let u8_content = content.as_bytes();
            Ok(u16::from_ne_bytes(
                u8_content
                    .try_into()
                    .map_err(|_| RustydomoError::ConversionError)?,
            ))
        } else {
            Err(RustydomoError::ConversionError)
        }
    }
}
