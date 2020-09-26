#![allow(dead_code)]

extern crate bytes;
extern crate futures;

mod server;

use crate::server::Server;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Result<T> = std::io::Result<T>;

// Messages passed on network.
struct RequestMessage {
    client: ClientIdentifier,
    server: ServerIdentifier,
    service_method: String,
    arg: Bytes,

    reply_channel: Sender<Result<ReplyMessage>>,
}

type ReplyMessage = Bytes;

type ServerIdentifier = String;
type ClientIdentifier = String;

// Client interface, used by the RPC client.
struct Client {
    client: ClientIdentifier,
    server: ServerIdentifier,

    request_bus: Sender<RequestMessage>,
}

struct Network {
    // Settings.
    reliable: bool,
    long_delays: bool,
    long_reordering: bool,

    // Clients
    clients: HashMap<ClientIdentifier, (bool, ServerIdentifier)>,
    servers: HashMap<ServerIdentifier, Arc<Server>>,

    // Network bus
    request_bus: Sender<RequestMessage>,
    request_pipe: Option<Receiver<RequestMessage>>,

    // Closing signal.
    keep_running: bool,

    // RPC Counter, using Cell for interior mutability.
    rpc_count: std::cell::Cell<usize>,
}

impl Network {
    const SHUTDOWN_DELAY: Duration = Duration::from_micros(20);

    pub fn set_reliable(&mut self, yes: bool) {
        self.reliable = yes
    }

    pub fn set_long_reordering(&mut self, yes: bool) {
        self.long_reordering = yes
    }

    pub fn set_long_delays(&mut self, yes: bool) {
        self.long_delays = yes
    }

    pub fn stop(&mut self) {
        self.keep_running = false;
    }

    pub fn make_connection(_server_name: ServerIdentifier) -> Client {
        unimplemented!()
    }

    fn dispatch(&self, client: &ClientIdentifier) -> Result<Arc<Server>> {
        let (enabled, server_name) =
            self.clients.get(client).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    format!("Client {} is not connected.", client),
                )
            })?;
        if !enabled {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                format!("Client {} is disabled.", client),
            ));
        }
        let server = self.servers.get(server_name).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "Cannot connect {} to server {}: server not found.",
                    client, server_name,
                ),
            )
        })?;

        Ok(server.clone())
    }

    pub fn get_total_rpc_count(&self) -> usize {
        self.rpc_count.get()
    }

    pub fn run_daemon() -> Arc<Mutex<Network>> {
        let mut network = Network::new();
        let rx = network.request_pipe.take().unwrap();

        let network = Arc::new(Mutex::new(network));

        let thread_pool = futures::executor::ThreadPool::builder()
            .pool_size(20)
            .name_prefix("network")
            .create()
            .expect("Creating network thread pool should not fail");

        let other = network.clone();
        std::thread::spawn(move || {
            let network = other;
            let mut stop_timer = Instant::now();
            loop {
                // If the lock of network is unfair, we could starve threads
                // trying to add / remove RPC servers, or change settings.
                // Having a shutdown delay helps minimise lock holding.
                if stop_timer.elapsed() >= Self::SHUTDOWN_DELAY {
                    let locked_network = network
                        .lock()
                        .expect("Network mutex should not be poisoned");
                    if !locked_network.keep_running {
                        break;
                    }
                    stop_timer = Instant::now();
                }

                match rx.try_recv() {
                    Ok(request) => {
                        let network_clone = network.clone();
                        thread_pool.spawn_ok(async move {
                            let server_result = {
                                let network = network_clone.lock().expect(
                                    "Network mutex should not be poisoned",
                                );
                                network.increase_rpc_count();

                                network.dispatch(&request.client)
                            };

                            // Cannot use server_result.amp() here, since there
                            // is an async request in the middle.
                            let reply = match server_result {
                                Ok(server) => {
                                    // Simulates the copy from network to server.
                                    let data = request.arg.clone();
                                    server
                                        .dispatch(request.service_method, data)
                                        .await
                                }
                                Err(e) => Err(e),
                            };

                            if let Err(_e) = request.reply_channel.send(reply) {
                                // TODO(ditsing): log and do nothing.
                            }
                        })
                    }
                    // All senders have disconnected. This should never happen,
                    // since the network instance itself holds a sender.
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => {
                        std::thread::sleep(Self::SHUTDOWN_DELAY)
                    }
                }
            }

            // rx is dropped here, all clients should get disconnected error
            // and stop sending messages.
        });

        network
    }
}

impl Network {
    fn increase_rpc_count(&self) {
        self.rpc_count.set(self.rpc_count.get() + 1);
    }

    fn new() -> Self {
        // The channel has infinite buffer, could OOM the server if there are
        // too many pending RPCs to be served.
        let (tx, rx) = channel();
        Network {
            reliable: false,
            long_delays: false,
            long_reordering: false,
            clients: Default::default(),
            servers: Default::default(),
            request_bus: tx,
            request_pipe: Some(rx),
            keep_running: true,
            rpc_count: std::cell::Cell::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_network() -> Network {
        Network::new()
    }

    #[test]
    fn rpc_count_works() {
        let network = make_network();
        assert_eq!(0, network.get_total_rpc_count());

        network.increase_rpc_count();
        assert_eq!(1, network.get_total_rpc_count());
    }
}
