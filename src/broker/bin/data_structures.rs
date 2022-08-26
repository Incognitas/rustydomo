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
            // create fixed array to receive  the value to convert
            let mut fixed_array: [u8; 2] = Default::default();
            // prepare slice of u8
            let u8_content = content.as_bytes();
            // copy the 2 bytes to convert
            fixed_array.copy_from_slice(&u8_content[0..2]);
            // return the obtained value
            Ok(u16::from_ne_bytes(fixed_array))
        } else {
            Err(RustydomoError::ConversionError)
        }
    }
}

impl TryInto<u32> for MessageHelper {
    type Error = RustydomoError;

    fn try_into(self) -> Result<u32, Self::Error> {
        if let Some(content) = self.m.as_str() {
            // create fixed array to receive  the value to convert
            let mut fixed_array: [u8; 4] = Default::default();
            // prepare slice of u8
            let u8_content = content.as_bytes();
            // copy the 2 bytes to convert
            fixed_array.copy_from_slice(&u8_content[0..4]);
            // return the obtained value
            Ok(u32::from_ne_bytes(fixed_array))
        } else {
            Err(RustydomoError::ConversionError)
        }
    }
}

pub enum ClientInteractionType {
    Request = 0x01,
    Partial = 0x02,
    Final = 0x03,
}

pub enum WorkerInteractionType {
    Ready = 0x01,
    Request = 0x02,
    Partial = 0x03,
    Final = 0x04,
}
