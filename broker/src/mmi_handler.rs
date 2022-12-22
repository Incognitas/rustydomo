use crate::{data_structures::ClientInteractionType, majordomo_context::MajordomoContext};

pub fn is_mmi_service(service_name: &str) -> bool {
    return service_name.starts_with("mmi.");
}

fn send_mmi_answer(connection: &zmq::Socket, client_id: &[u8], service_name: &str, answer: &str) {
    let final_request_response: [u8; 1] = [ClientInteractionType::Final as u8];
    connection.send(&client_id, zmq::SNDMORE).unwrap();
    connection.send("MDPC02".as_bytes(), zmq::SNDMORE).unwrap();
    connection
        .send(&final_request_response.as_slice(), zmq::SNDMORE)
        .unwrap();
    connection.send(&service_name, zmq::SNDMORE).unwrap();
    connection.send(&answer, 0).unwrap();
}

pub fn handle_mmi_services(
    ctx: &MajordomoContext,
    service_name: &str,
    client_id: &[u8],
    clients_connection: &zmq::Socket,
) -> bool {
    if !is_mmi_service(&service_name) {
        // nothing to do it it is not an mmi service
        false
    } else {
        let mut remaining_payload: Vec<Vec<u8>> = Vec::new();

        // first of all read all remaining frames left from client
        if let Ok(has_more_data) = clients_connection.get_rcvmore() {
            if has_more_data {
                remaining_payload = clients_connection.recv_multipart(0).unwrap();
            }
        }
        log::debug!("Handling MMI request: {}", &service_name);
        match service_name {
            "mmi.service" => handle_mmi_service_request(
                ctx,
                &client_id,
                &service_name,
                &clients_connection,
                remaining_payload,
            ),
            _ => {
                log::warn!("Unrecognized service : {}", service_name);
                send_mmi_answer(&clients_connection, &client_id, &service_name, "501");
                false
            }
        }
    }
}

fn handle_mmi_service_request(
    ctx: &MajordomoContext,
    client_id: &[u8],
    service_name: &str,
    clients_connection: &zmq::Socket,
    payload: Vec<Vec<u8>>,
) -> bool {
    if payload.len() >= 1 {
        // we expect at least one parameter : the service name
        // if it is not the case we just search for an invalid service and it will simply fail
        let service_to_search =
            String::from_utf8(payload[0].clone()).unwrap_or("__unknown_service__".into());
        if !ctx.can_handle_service(&service_to_search) {
            send_mmi_answer(&clients_connection, &client_id, &service_name, "404");
        } else {
            send_mmi_answer(&clients_connection, &client_id, &service_name, "200");
        }
        true
    } else {
        // if there is no payload we just indicate the error accordingly
        log::warn!("No parameter passed to mmi.service, ignoring request");
        false
    }
}
