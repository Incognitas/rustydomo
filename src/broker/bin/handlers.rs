use crate::data_structures::{ConnectionData, MessageHelper};
use crate::errors::RustydomoError;
use crate::majordomo_context::MajordomoContext;
use log::{debug, info, warn};
use zmq::{Message, Socket};

static EXPECTED_VERSION_HEADER: &str = "MDPC02";

fn receive_data(sock: &Socket) -> Result<Message, RustydomoError> {
    let msg = sock
        .recv_msg(0)
        .map_err(|err| RustydomoError::CommunicationError(err.to_string()));

    msg
}

enum CommandType {
    Request = 0x01,
}

pub fn handle_client_messages(
    sock: &ConnectionData,
    ctx: &mut MajordomoContext,
) -> Result<(), RustydomoError> {
    // A REQUEST command consists of a multipart message of 4 or more frames, formatted on the wire as follows:
    // Frame 0: “MDPC02” (six bytes, representing MDP/Client v0.2)
    // Frame 1: 0x01 (one byte, representing REQUEST)
    // Frame 2: Service name (printable string)
    // Frames 3+: Request body (opaque binary)

    let mut content = receive_data(&sock.connection)?;
    // note : *content converts Message to &[u8]  using Deref trait, so we can process it directly this way

    // skip to the next empty frame in the multiple parts received
    while (*content).len() != 0 && content.get_more() {
        content = receive_data(&sock.connection)?;
    }

    // make sure there are more parts waiting afterwards
    assert!(content.get_more());
    // finally retrieve the first element of the actual content

    {
        // frame 0 read and handled here
        let content = receive_data(&sock.connection)?;
        // TODO: Read all parts at once instead of iteratively because we have to keep track of
        // identify envelop and everything (there is an API for that in rust::zmq

        // ensure that we are reading a valid MDP client signa by checking its headerl
        {
            let obtained = content.as_str().unwrap();
            if obtained != EXPECTED_VERSION_HEADER {
                return Err(RustydomoError::CommunicationError(std::format!(
                    "Unrecognized protocol frame received. Expected '{}', Obtained '{}'",
                    EXPECTED_VERSION_HEADER,
                    obtained
                )));
            }
        }
    }

    {
        // frame 1 : command type
        let content = receive_data(&sock.connection)?;

        match (*content)[0] {
            x if x == CommandType::Request as u8 => debug!("Received client request"),
            val => return Err(RustydomoError::UnrecognizedCommandType(val)),
        }
    }

    {
        // frame 2 : service name
        let content = receive_data(&sock.connection)?;
        let service_name = content.as_str().unwrap();
        debug!("Service name called : {}", service_name);
        if !ctx.can_handle_service(service_name) {
            return Err(RustydomoError::ServiceNotAvailable(service_name.into()));
        }
    }

    {
        // next frames are service-specific
    }
    // safe guard to finish reading all parts if it is not already done
    if content.get_more() {
        warn!("Some residual frames are present ! This must be handled !");
        let mut content = receive_data(&sock.connection)?;
        while content.get_more() {
            content = receive_data(&sock.connection)?;
        }
    }

    Ok(())
}

pub fn handle_service_messages(_sock: &ConnectionData) -> Result<(), RustydomoError> {
    todo!()
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
