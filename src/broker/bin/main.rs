use env_logger::Env;
use log::info;
use zmq::Context;

mod broker_connection;

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    info!("Welcome to The Majordomo Broker");
    let zmq_ctx = Context::new();
    info!("Creating clients related connection...");
    let router_connection = broker_connection::bind_router_connection(
        &zmq_ctx,
        "tcp://*:5000",
        "inproc://monitor_clients_router",
    )
    .expect("Failed to create clients connection");

    info!("Creating services related connection...");
    let dealer_connection = broker_connection::bind_router_connection(
        &zmq_ctx,
        "tcp://*:6000",
        "inproc://monitor_services_router",
    )
    .expect("Failed to create services related connection");
}
