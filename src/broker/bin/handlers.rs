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

// generic monitor handlers
fn handle_monitor_message(source_name: &str, sock: &ConnectionData) -> Result<(), RustydomoError> {
    let content = receive_data(&sock.monitor_connection)?;
    if content.get_more() {
        //read eventual extra data beforehand
        let origin_addr = receive_data(&sock.monitor_connection)?;

        // connection address received on second frame (as defined by zmq specification)

        let content_helper = MessageHelper { m: content };

        if let Ok(event_id) = <MessageHelper as TryInto<u16>>::try_into(content_helper) {
            match event_id {
                x if x == zmq::SocketEvent::HANDSHAKE_SUCCEEDED as u16 => {
                    info!(
                        "{} accepted on {}",
                        source_name,
                        origin_addr.as_str().unwrap_or("<unknown>")
                    )
                }
                // x if x == zmq::SocketEvent::CLOSED as u16 => {
                //     info!("{} connection closed", source_name)
                // }
                x if x == zmq::SocketEvent::DISCONNECTED as u16 => {
                    info!(
                        "{} disconnected from {}",
                        source_name,
                        origin_addr.as_str().unwrap_or("<unknown>")
                    )
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
