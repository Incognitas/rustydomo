use crate::data_structures::{ConnectionData, MessageHelper};
use crate::errors::RustydomoError;
use log::{debug, info};
use zmq::{Message, Socket};

fn receive_data(sock: &Socket) -> Result<Message, RustydomoError> {
    let msg = sock
        .recv_msg(0)
        .map_err(|err| RustydomoError::CommunicationError(err.to_string()));
    msg
}

pub fn handle_client_messages(_sock: &ConnectionData) -> Result<(), RustydomoError> {
    Ok(())
}

pub fn handle_service_messages(_sock: &ConnectionData) -> Result<(), RustydomoError> {
    Ok(())
}

// monitor handlers

pub fn handle_client_monitor_messages(sock: &ConnectionData) -> Result<(), RustydomoError> {
    let content = receive_data(&sock.monitor_connection)?;
    if content.get_more() {
        debug!("More data to receive on clients monitor");
        //read eventual extra data beforehand
        let value = receive_data(&sock.monitor_connection)?;
        let content_helper = MessageHelper { m: content };

        if let Ok(event_id) = <MessageHelper as TryInto<u16>>::try_into(content_helper) {
            match event_id {
                x if x == zmq::SocketEvent::HANDSHAKE_SUCCEEDED as u16 => info!(
                    "Client accepted : {}",
                    value.as_str().unwrap().chars().next().unwrap() as u8
                ),
                x if x == zmq::SocketEvent::CLOSED as u16 => info!("Client connection closed"),
                x if x == zmq::SocketEvent::DISCONNECTED as u16 => {
                    info!("Client unexpectidely disconnected")
                }
                _ => (),
            }
        }
    }
    Ok(())
}

pub fn handle_service_monitor_messages(_sock: &ConnectionData) -> Result<(), RustydomoError> {
    Ok(())
}
