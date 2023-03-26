use domolib::client::Client;
use domolib::client::ClientRequestState;
use env_logger::Env;

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let client = Client::new("tcp://127.0.0.1:5000").unwrap();
    let params: Vec<Vec<u8>> = vec![b"UBER_PARAM".to_vec()];
    let result_request = client.send_request("UBER_SERVICE", &params);

    if let Some(request) = result_request {
        for entry in request {
            match entry.state {
                ClientRequestState::PARTIAL => {
                    log::debug!("Partial answer received");
                }
                ClientRequestState::FINAL => {
                    log::debug!("Final answer received");
                }
                _ => (), // ignore other states
            }
        }
    }
}
