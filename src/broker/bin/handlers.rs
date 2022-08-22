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
fn handle_monitor_message(source_name: &str, sock: &ConnectionData) -> Result<(), RustydomoError> {
    debug!("Monitoring event received on {}", source_name);
    let content = receive_data(&sock.monitor_connection)?;
    if content.get_more() {
        //read eventual extra data beforehand
        let _value: u32 = MessageHelper {
            m: receive_data(&sock.monitor_connection)?,
        }
        .try_into()?; // convert to u32 and make sure it went well
        let content_helper = MessageHelper { m: content };

        if let Ok(event_id) = <MessageHelper as TryInto<u16>>::try_into(content_helper) {
            match event_id {
                x if x == zmq::SocketEvent::HANDSHAKE_SUCCEEDED as u16 => {
                    info!("{} accepted", source_name)
                }
                x if x == zmq::SocketEvent::CLOSED as u16 => {
                    info!("{} connection closed", source_name)
                }
                x if x == zmq::SocketEvent::DISCONNECTED as u16 => {
                    info!("{} disconnected", source_name)
                }
                _ => debug!("Unrecognized event : {}", event_id),
            }
        } else {
            debug!("Failed to convert event_id to u16");
        }
    }
    Ok(())
}

pub fn handle_client_monitor_messages(sock: &ConnectionData) -> Result<(), RustydomoError> {
    handle_monitor_message("Client", sock)
}

pub fn handle_service_monitor_messages(sock: &ConnectionData) -> Result<(), RustydomoError> {
    handle_monitor_message("Service", sock)
}
