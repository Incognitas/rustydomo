use crate::data_structures::{
    ClientInteractionType, ConnectionData, Identity, WorkerInteractionType,
};
use crate::majordomo_context::MajordomoContext;
use crate::mmi_handler::{handle_mmi_services, is_mmi_service};
use domolib::errors::RustydomoError;
use domolib::structures::MessageHelper;
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

pub fn handle_client_messages(
    clients_connection: &ConnectionData,
    workers_connection: &ConnectionData,
    ctx: &mut MajordomoContext,
) -> Result<(), RustydomoError> {
    // A REQUEST command consists of a multipart message of 4 or more frames, formatted on the wire as follows:
    // Frame 0: “MDPC02” (six bytes, representing MDP/Client v0.2)
    // Frame 1: 0x01 (one byte, representing REQUEST)
    // Frame 2: Service name (printable string)
    // Frames 3+: Request body (opaque binary)
    let mut final_payload: Vec<Vec<u8>> = Vec::new();

    // TODO: replace iter used to fetch command with a single read multipart which fills a
    // Vec<Vec<u8>>. It it will be more optimal

    // finally retrieve the first element of the actual content : the client id
    let client_id = receive_data(&clients_connection.connection)?;
    let id = Identity::try_from(&(*client_id)).unwrap();
    log::debug!("Client {:08X} connected", id.value);

    // ensure that we are reading a valid MDP client signa by checking its header
    {
        // frame 0 read and handled here
        let content = receive_data(&clients_connection.connection)?;
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
    let content = receive_data(&clients_connection.connection)?;
    let command_type = (*content)[0];

    match command_type {
        x if x == ClientInteractionType::Request as u8 => {
            debug!("Received client request");

            let worker_command_type: [u8; 1] = [WorkerInteractionType::Request as u8];
            // convert client resquet to worker request
            final_payload.push(worker_command_type.to_vec());
            // do not forget to add the client id in the frame to send
            final_payload.push((*client_id).to_vec());
            // then an  empty frame
            final_payload.push(vec![]);
        }
        val => return Err(RustydomoError::UnrecognizedCommandType(val)),
    }

    // frame 2 : service name
    let mut content = receive_data(&clients_connection.connection)?;
    let service_name = content.as_str().unwrap().to_string();
    debug!("Service name called : {}", service_name);

    // check whether or not we have to handle an MMI request before
    if is_mmi_service(&service_name) {
        debug!("MMI service");
        if !handle_mmi_services(
            &ctx,
            &service_name,
            &(*client_id),
            &clients_connection.connection,
        ) {
            // at this point we can just send the payload to be handled to context
            // next frames are service-specific
            loop {
                if content.get_more() {
                    content = receive_data(&clients_connection.connection)?;
                    final_payload.push((*content).to_vec());
                    debug!("Extra frame provided as service specific information");
                } else {
                    break;
                }
            }
        }
    } else {
        // simple check to ensure we can handle this properly
        if !ctx.can_handle_service(&service_name) {
            debug!("Can no handle service '{service_name}'");
            return Err(RustydomoError::ServiceNotAvailable(service_name.into()));
        }
        ctx.send_task_to_worker(&workers_connection.connection, service_name, final_payload)?;
    }
    Ok(())
}

#[allow(dead_code)]
fn display_content(entry: &[u8]) {
    log::debug!(
        "{}",
        entry
            .iter()
            .map(|val| std::format!("{:02X}", val))
            .collect::<String>()
    );
}

fn send_residual_data(
    sock_to_read: &zmq::Socket,
    sock_to_send_to: &zmq::Socket,
) -> Result<(), RustydomoError> {
    loop {
        let data = receive_data(&sock_to_read)?;
        let has_more = data.get_more();
        sock_to_send_to
            .send(data, if has_more { zmq::SNDMORE } else { 0 })
            .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
        if !has_more {
            break;
        }
    }
    Ok(())
}

fn handle_worker_partial_final_answer(
    workers_socket: &zmq::Socket,
    clients_socket: &zmq::Socket,
    response_type: ClientInteractionType,
) -> Result<(), RustydomoError> {
    let has_payload: bool;

    loop {
        let client_identity = receive_data(&workers_socket)?;

        // while we are not on an emlpty frame, we continue to send "as is" the frames we receive as they
        // are considered to be part of the identity packets the client sent
        if client_identity.len() != 0 {
            clients_socket
                .send(client_identity, zmq::SNDMORE)
                .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
        } else {
            has_payload = client_identity.get_more();
            break;
        }
    }

    clients_socket
        .send::<&[u8]>("MDPC02".as_bytes(), zmq::SNDMORE)
        .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;

    let data_to_send: [u8; 1] = [response_type as u8];

    if has_payload {
        clients_socket
            .send(data_to_send.as_slice(), zmq::SNDMORE)
            .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;

        send_residual_data(&workers_socket, &clients_socket)?;
    } else {
        clients_socket
            .send(data_to_send.as_slice(), 0)
            .map_err(|err| RustydomoError::CommunicationError(err.to_string()))?;
    }
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
    workers_connection: &ConnectionData,
    ctx: &mut MajordomoContext,
) -> Result<(), RustydomoError> {
    // Frame 0: “MDPW02” (six bytes, representing MDP/Worker v0.2)
    // Frame 1: (one byte, representing READY / REQUEST / HEARTBEAT)
    // next frames are context specific and application specific
    let worker_identity = receive_data(&workers_connection.connection)?;
    assert!(worker_identity.len() == 5);

    // parse command type (frame 0)
    let content = receive_data(&workers_connection.connection)?;
    let obtained = content.as_str().unwrap();
    if obtained != EXPECTED_WORKER_VERSION_HEADER {
        return Err(RustydomoError::CommunicationError(std::format!(
            "Unrecognized worker protocol frame received. Expected '{}', Obtained '{}'",
            EXPECTED_WORKER_VERSION_HEADER,
            obtained
        )));
    }

    // read frame 1 (command type)
    let content = receive_data(&workers_connection.connection)?;

    match (*content)[0] {
        x if x == WorkerInteractionType::Ready as u8 => {
            let service_name = receive_data(&workers_connection.connection)?;
            ctx.register_worker(&worker_identity, service_name.as_str().unwrap())?;
        }
        x if x == WorkerInteractionType::Heartbeat as u8 => {
            // heartbeat are quite easy to handle here
            ctx.refresh_expiration_time(&worker_identity)?;
        }
        x if x == WorkerInteractionType::Partial as u8 => {
            // any time we receive a command from worker, refresh its expiration time ( not only
            // on heartbeat)
            ctx.refresh_expiration_time(&worker_identity)?;
            handle_worker_partial_final_answer(
                &workers_connection.connection,
                &clients_connection.connection,
                ClientInteractionType::Partial,
            )?;
        }
        x if x == WorkerInteractionType::Final as u8 => {
            // any time we receive a command from worker, refresh its expiration time ( not only
            // on heartbeat)
            ctx.refresh_expiration_time(&worker_identity)?;
            handle_worker_partial_final_answer(
                &workers_connection.connection,
                &clients_connection.connection,
                ClientInteractionType::Final,
            )?;
        }
        x if x == WorkerInteractionType::Disconnect as u8 => {
            ctx.remove_worker(&worker_identity).unwrap_or_else(|err| {
                log::warn!(
                    "Error while trying to remove worker from list of known workers {}",
                    err.to_string()
                )
            });
        }

        _ => todo!(), // should never happen
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
