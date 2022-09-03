use crate::errors::RustydomoError;
use zmq::{Message, Socket};

pub enum SocketType {
    ClientSocket = 0,
    ClientMonitorSocket,
    ServiceSocket,
    WorkerMonitorSocket,
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
            x if x == SocketType::WorkerMonitorSocket as usize => {
                Ok(SocketType::WorkerMonitorSocket)
            }
            val => Err(RustydomoError::Unknown(std::format!(
                "Unknown socket type : {}",
                val
            ))),
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
            Err(RustydomoError::ConversionError(
                "Failed to convert value to u16".to_string(),
            ))
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
            Err(RustydomoError::ConversionError(
                "Failed to convert value to u32".to_string(),
            ))
        }
    }
}

#[derive(Clone, PartialEq, Copy)]
pub struct Identity {
    pub value: u32, // the only members that matter
    _private: (),   // make sure this can not be instanciated directly }
}

impl TryFrom<&[u8]> for Identity {
    type Error = RustydomoError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 4 || value.len() > 5 {
            Err(RustydomoError::ConversionError(
                "Identity conversion failed".to_string(),
            ))
        } else {
            let mut array_to_parse: [u8; 4] = Default::default();
            // identity is sent as an array of 5 bytes
            // 0x00 + 4 random bytes
            // so we have to take care if ignoring the leading 0 and start at offset 1
            array_to_parse.copy_from_slice(&value[value.len() - 5..]);
            Ok(Identity {
                value: u32::from_ne_bytes(array_to_parse),
                _private: (),
            })
        }
    }
}

impl Into<Vec<u8>> for Identity {
    fn into(self) -> Vec<u8> {
        // the resturned identity SHALL be 5 bytes logs, not 4
        let array = self.value.to_ne_bytes();
        let mut result: Vec<u8> = vec![0];
        result.extend_from_slice(&array);
        result
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
    Heartbeat = 0x05,
}
