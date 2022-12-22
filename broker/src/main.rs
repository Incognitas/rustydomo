mod broker_connection;
mod data_structures;
mod errors;
mod handlers;
mod majordomo_context;
mod mmi_handler;

use crate::errors::RustydomoError;
use data_structures::SocketType;
use env_logger::Env;
use log::info;
use majordomo_context::MajordomoContext;
use zmq::Context;

fn main() -> ! {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    println!("{}", "001".parse::<u32>().unwrap());
    info!("Welcome to The Majordomo Broker");
    let zmq_ctx = Context::new();
    info!("Creating clients related connection...");
    let clients_connection = broker_connection::bind_router_connection(
        &zmq_ctx,
        "tcp://*:5000",
        "inproc://monitor_clients_router",
    )
    .expect("Failed to create clients connection");

    info!("Creating services related connection...");
    let workers_connection = broker_connection::bind_router_connection(
        &zmq_ctx,
        "tcp://*:6000",
        "inproc://monitor_services_router",
    )
    .expect("Failed to create services related connection");

    let mut ctx = MajordomoContext::new();

    loop {
        let sockets_stimulated = {
            let mut poll_list = [
                clients_connection.connection.as_poll_item(zmq::POLLIN),
                clients_connection
                    .monitor_connection
                    .as_poll_item(zmq::POLLIN),
                workers_connection.connection.as_poll_item(zmq::POLLIN),
                workers_connection
                    .monitor_connection
                    .as_poll_item(zmq::POLLIN),
            ];

            match zmq::poll(&mut poll_list, 400) {
                Ok(_) => {
                    // V1 => filter then map
                    /*Ok(poll_list
                    .into_iter()
                    .enumerate()
                    .filter(|(_, entry)| -> bool { entry.get_revents() == zmq::POLLIN })
                    .map(|(idx, _)| SocketType::try_from(idx).expect("invalid socket type"))
                    .collect::<Vec<SocketType>>())
                    */
                    // V2 => filter_map directly
                    Ok(poll_list
                        .into_iter()
                        .enumerate()
                        .filter_map(|(idx, entry)| -> Option<data_structures::SocketType> {
                            // if there are events on curent connection, just save the socket type
                            // so that it can be fetched afterwards

                            if entry.get_revents() & zmq::POLLIN == zmq::POLLIN {
                                Some(
                                    data_structures::SocketType::try_from(idx)
                                        .expect("invalid socket type"),
                                )
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<data_structures::SocketType>>())
                }
                Err(err) => Err(RustydomoError::Unknown(err.to_string())),
            }
        };

        match sockets_stimulated {
            Ok(sockets_to_use) => sockets_to_use.iter().for_each(|type_| match type_ {
                SocketType::ClientSocket => handlers::handle_client_messages(
                    &clients_connection,
                    &workers_connection,
                    &mut ctx,
                )
                .expect("Failed to handle client message"),
                SocketType::ClientMonitorSocket => {
                    handlers::handle_client_monitor_messages(&clients_connection)
                        .expect("Failed to handle client monitor message")
                }
                SocketType::ServiceSocket => handlers::handle_worker_messages(
                    &clients_connection,
                    &workers_connection,
                    &mut ctx,
                )
                .expect("Failed to handle service message"),
                SocketType::WorkerMonitorSocket => {
                    handlers::handle_worker_monitor_messages(&workers_connection)
                        .expect("Failed to handle service monitor message")
                }
            }),
            _ => log::debug!("Erreur hein ?"),
        };

        ctx.check_expired_workers();
        ctx.send_heartbeat(&workers_connection.connection).unwrap();
    }
}
