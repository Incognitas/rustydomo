use domolib::errors::RustydomoError;
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

#[derive(Clone, PartialEq)]
pub struct Identity {
    pub value: Vec<u8>, // the only members that matter
    _private: (),       // make sure this can not be instanciated directly
}

impl TryFrom<&[u8]> for Identity {
    type Error = RustydomoError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        // identity is sent as an array of X bytes

        Ok(Identity {
            value: value.to_vec(),
            _private: (),
        })
    }
}

impl Into<Vec<u8>> for Identity {
    fn into(self) -> Vec<u8> {
        self.value
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
    Disconnect = 0x06,
}
