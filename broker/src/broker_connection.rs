use crate::data_structures::ConnectionData;
use domolib::errors::RustydomoError;

use log::{debug, info, log_enabled, Level};
use zmq::{Context, SocketEvent, SocketType};
/// .
///
/// # Errors
///
/// This function will return an error if .
pub fn bind_router_connection(
    ctx: &Context,
    router_connection_string: &str,
    monitor_connection_string: &str,
) -> Result<ConnectionData, RustydomoError> {
    let router_socket = ctx
        .socket(SocketType::ROUTER)
        .map_err(|err| RustydomoError::SocketCreationError(err.to_string()))?;

    router_socket
        .bind(router_connection_string)
        .map_err(|err| RustydomoError::SocketBindingError(err.to_string()))?;

    let events_to_follow =
        SocketEvent::DISCONNECTED as i32 | SocketEvent::HANDSHAKE_SUCCEEDED as i32;
    router_socket
        .monitor(monitor_connection_string, events_to_follow)
        .map_err(|err| -> RustydomoError {
            RustydomoError::MonitorCreationError(err.to_string())
        })?;

    // at this point the Pair is created and the monitor is active. We just have
    // to create the connection to this monitor properly
    let monitor_connection = ctx
        .socket(SocketType::PAIR)
        .map_err(|err| RustydomoError::MonitorCreationError(err.to_string()))?;

    monitor_connection
        .connect(monitor_connection_string)
        .map_err(|err| RustydomoError::MonitorCreationError(err.to_string()))?;

    info!("Listening to connections on '{}'", router_connection_string);
    if log_enabled!(Level::Debug) {
        debug!(
            "Monitor for this connection created on '{}'",
            monitor_connection_string
        );
    }
    return Ok(ConnectionData {
        connection: router_socket,
        monitor_connection,
    });
}
