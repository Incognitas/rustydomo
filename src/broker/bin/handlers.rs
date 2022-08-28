use std::collections::VecDeque;

use crate::data_structures::{
    ClientInteractionType, ConnectionData, MessageHelper, WorkerInteractionType,
};
use crate::errors::RustydomoError;
use crate::majordomo_context::MajordomoContext;
use log::{debug, info};
use zmq::{Message, Socket};

static EXPECTED_CLIENT_VERSION_HEADER: &str = "MDPC02";
static EXPECTED_WORKER_VERSION_HEADER: &str = "MDPW02";

fn receive_data(sock: &Socket) -> Result<Message, RustydomoError> {
    let msg = sock
        .recv_msg(0)
        .map_err(|err| RustydomoError::CommunicationError(err.to_string()));

    msg
}

fn extract_address_envelop(sock: &zmq::Socket) -> Result<VecDeque<Vec<u8>>, RustydomoError> {
    let mut address_envelope: VecDeque<Vec<u8>> = VecDeque::new();

    let mut content = receive_data(&sock)?;
    // note : *content converts Message to &[u8]  using Deref trait, so we can process it directly this way
    address_envelope.push_back((*content).to_vec());

    // skip to the next empty frame in the multiple parts received
    while (*content).len() != 0 && content.get_more() {
        content = receive_data(&sock)?;
        address_envelope.push_back((*content).to_vec());
    }
    // also add the empty frame to the address envelope
    address_envelope.push_back((*content).to_vec());

    if !content.get_more() {
        return Err(RustydomoError::CommunicationError(
            "Not enough frames received".into(),
        ));
    }
    Ok(address_envelope)
}

pub fn handle_client_messages(
    client_connection: &ConnectionData,
    ctx: &mut MajordomoContext,
) -> Result<(), RustydomoError> {
    // A REQUEST command consists of a multipart message of 4 or more frames, formatted on the wire as follows:
    // Frame 0: “MDPC02” (six bytes, representing MDP/Client v0.2)
    // Frame 1: 0x01 (one byte, representing REQUEST)
    // Frame 2: Service name (printable string)
    // Frames 3+: Request body (opaque binary)
    let mut final_payload: Vec<Vec<u8>> = Vec::new();

    let address_envelope = extract_address_envelop(&client_connection.connection)?;
    final_payload.extend(address_envelope);
    // finally retrieve the first element of the actual content

    // frame 0 read and handled here
    let content = receive_data(&client_connection.connection)?;

    // ensure that we are reading a valid MDP client signa by checking its headerl
    {
        let obtained = content.as_str().unwrap();
        if obtained != EXPECTED_CLIENT_VERSION_HEADER {
            return Err(RustydomoError::CommunicationError(std::format!(
                "Unrecognized protocol frame received. Expected '{}', Obtained '{}'",
                EXPECTED_CLIENT_VERSION_HEADER,
                obtained
            )));
        }
    }

    // frame 1 : command type
    let content = receive_data(&client_connection.connection)?;
    let command_type = (*content)[0];

    match command_type {
        x if x == ClientInteractionType::Request as u8 => {
            debug!("Received client request");
            let worker_command_type: [u8; 1] = [WorkerInteractionType::Request as u8];
            // convert client requet to worker request
            final_payload.push(worker_command_type[..1].to_vec());
        }
        val => return Err(RustydomoError::UnrecognizedCommandType(val)),
    }

    // frame 2 : service name
    let content = receive_data(&client_connection.connection)?;
    let service_name = content.as_str().unwrap();
    debug!("Service name called : {}", service_name);
    if !ctx.can_handle_service(service_name) {
        return Err(RustydomoError::ServiceNotAvailable(service_name.into()));
    }

    // dispatch frame to required service

    // next frames are service-specific
    if content.get_more() {
        while content.get_more() {
            debug!("Extra frame provided as service specific information");
            let content = receive_data(&client_connection.connection)?;
            final_payload.push((*content).to_vec());
        }
    }

    // at this point we can just send the payload to be handled to context
    ctx.queue_task(service_name, final_payload)?;

    Ok(())
}

///
/// Handles all messges sent by workers$
///
/// # Arguments
///
/// * `clients_connection` - socket used to receive data from workers
/// * `worker_connection` - socket used to receive data from workers
/// * `ctx` - context linked to Majordomo handling
///

pub fn handle_worker_messages(
    clients_connection: &ConnectionData,
    worker_connection: &ConnectionData,
    ctx: &mut MajordomoContext,
) -> Result<(), RustydomoError> {
    // Frame 0: “MDPW02” (six bytes, representing MDP/Worker v0.2)
    // Frame 1: (one byte, representing READY / REQUEST / HEARTBEAT)
    // next frames are context specific and application specific
    let mut address_envelope = extract_address_envelop(&worker_connection.connection)?;

    // parse command type (frame 0)
    let content = receive_data(&worker_connection.connection)?;
    let obtained = content.as_str().unwrap();
    if obtained != EXPECTED_WORKER_VERSION_HEADER {
        return Err(RustydomoError::CommunicationError(std::format!(
            "Unrecognized worker protocol frame received. Expected '{}', Obtained '{}'",
            EXPECTED_WORKER_VERSION_HEADER,
            obtained
        )));
    }

    // read frame 1 (command type)
    let content = receive_data(&worker_connection.connection)?;

    match (*content)[0] {
        x if x == WorkerInteractionType::Ready as u8 => {
            let service_name = receive_data(&worker_connection.connection)?;
            ctx.register_worker(
                address_envelope.get(0).unwrap().clone(),
                service_name.as_str().unwrap(),
            )?;
        }
        x if x == WorkerInteractionType::Partial as u8 => {
            // remove identity address added by the dealer of the worker
            address_envelope.pop_front().unwrap();
            // what is left in envelope is just the identity of the client which called the service
            clients_connection
                .connection
                .send_multipart(address_envelope.iter(), zmq::SNDMORE)
                .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;

            let data_to_send: [u8; 1] = [ClientInteractionType::Partial as u8];
            clients_connection
                .connection
                .send(&data_to_send[..1], 0 /* nop more frames to send */)
                .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
        }
        x if x == WorkerInteractionType::Final as u8 => {
            // remove identity address added by the dealer of the worker
            let _worker_identity = address_envelope.pop_front().unwrap();
            // what is left in envelope is just the identity of the client which called the service
            clients_connection
                .connection
                .send_multipart(address_envelope.iter(), zmq::SNDMORE)
                .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;

            let data_to_send: [u8; 1] = [ClientInteractionType::Final as u8];
            clients_connection
                .connection
                .send(&data_to_send[..1], 0 /* nop more frames to send */)
                .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
        }

        _ => todo!(),
    }
    Ok(())
}

// generic monitor handlers
fn handle_monitor_message(source_name: &str, sock: &ConnectionData) -> Result<(), RustydomoError> {
    let content = receive_data(&sock.monitor_connection)?;
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

    Ok(())
}

pub fn handle_client_monitor_messages(sock: &ConnectionData) -> Result<(), RustydomoError> {
    handle_monitor_message("Client", sock)
}

pub fn handle_worker_monitor_messages(sock: &ConnectionData) -> Result<(), RustydomoError> {
    handle_monitor_message("Worker", sock)
}
