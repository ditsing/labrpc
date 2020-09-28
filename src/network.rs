use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::{thread_rng, Rng};

use crate::{
    Client, ClientIdentifier, Result, RpcOnWire, Server, ServerIdentifier,
};

pub struct Network {
    // Settings.
    reliable: bool,
    long_delays: bool,
    long_reordering: bool,

    // Clients
    clients: HashMap<ClientIdentifier, (bool, ServerIdentifier)>,
    servers: HashMap<ServerIdentifier, Arc<Server>>,

    // Network bus
    request_bus: Sender<RpcOnWire>,
    request_pipe: Option<Receiver<RpcOnWire>>,

    // Closing signal.
    keep_running: bool,
    // Whether the network is active or not.
    is_running: bool,

    // RPC Counter, using Cell for interior mutability.
    rpc_count: std::cell::Cell<usize>,
}

impl Network {
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

    pub fn is_running(&self) -> bool {
        self.is_running
    }

    pub fn make_client<C: Into<ClientIdentifier>, S: Into<ServerIdentifier>>(
        &mut self,
        client: C,
        server: S,
    ) -> Client {
        let (client, server) = (client.into(), server.into());
        self.clients.insert(client.clone(), (true, server.clone()));
        Client {
            client,
            server,
            request_bus: self.request_bus.clone(),
        }
    }

    pub fn set_enable_client(&mut self, client: &ClientIdentifier, yes: bool) {
        self.clients.get_mut(client).map(|pair| pair.0 = yes);
    }

    pub fn add_server(
        &mut self,
        server_name: ServerIdentifier,
        server: Arc<Server>,
    ) {
        self.servers.insert(server_name, server);
    }

    pub fn remove_server(&mut self, server_name: &ServerIdentifier) {
        self.servers.remove(server_name);
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
}

impl Network {
    const MAX_MINOR_DELAY_MILLIS: u64 = 27;
    const MAX_SHORT_DELAY_MILLIS: u64 = 100;
    const MAX_LONG_DELAY_MILLIS: u64 = 7000;

    const DROP_RATE: (u32, u32) = (100, 1000);
    const LONG_REORDERING_RATE: (u32, u32) = (600u32, 900u32);

    const LONG_REORDERING_BASE_DELAY_MILLIS: u64 = 200;
    const LONG_REORDERING_RANDOM_DELAY_BOUND_MILLIS: u64 = 2000;

    const SHUTDOWN_DELAY: Duration = Duration::from_micros(20);

    async fn delay_for_millis(milli_seconds: u64) {
        tokio::time::delay_for(Duration::from_millis(milli_seconds)).await;
    }

    async fn serve_rpc(network: Arc<Mutex<Self>>, rpc: RpcOnWire) {
        let (server_result, reliable, long_reordering, long_delays) = {
            let network = network
                .lock()
                .expect("Network mutex should not be poisoned");
            network.increase_rpc_count();

            (
                network.dispatch(&rpc.client),
                network.reliable,
                network.long_reordering,
                network.long_delays,
            )
        };

        // Random delay before sending requests to server.
        if !reliable {
            let minor_delay =
                thread_rng().gen_range(0, Self::MAX_MINOR_DELAY_MILLIS);
            Self::delay_for_millis(minor_delay).await;

            // Random drop of a DROP_RATE / DROP_BASE chance.
            if thread_rng().gen_ratio(Self::DROP_RATE.0, Self::DROP_RATE.1) {
                // Note this is different from the original Go version.
                // Here we don't reply to client until timeout actually passes.
                Self::delay_for_millis(Self::MAX_MINOR_DELAY_MILLIS).await;

                let _ = rpc.reply_channel.send(Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Remote server did not respond in time.",
                )));
                return;
            }
        }

        let reply = match server_result {
            // Call the server.
            Ok(server) => {
                // Simulates the copy from network to server.
                let data = rpc.request.clone();
                server.dispatch(rpc.service_method, data).await
            }
            // If the server does not exist, return error after a random delay.
            Err(e) => {
                let long_delay = rand::thread_rng().gen_range(
                    0,
                    if long_delays {
                        Self::MAX_LONG_DELAY_MILLIS
                    } else {
                        Self::MAX_SHORT_DELAY_MILLIS
                    },
                );
                Self::delay_for_millis(long_delay).await;
                Err(e)
            }
        };

        if reply.is_ok() {
            // Random drop again.
            if !reliable
                && thread_rng().gen_ratio(Self::DROP_RATE.0, Self::DROP_RATE.1)
            {
                let _ = rpc.reply_channel.send(Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "The network did not send respond in time.",
                )));
                return;
            } else if long_reordering {
                let should_reorder = thread_rng().gen_ratio(
                    Self::LONG_REORDERING_RATE.0,
                    Self::LONG_REORDERING_RATE.1,
                );
                if should_reorder {
                    let long_delay_bound = thread_rng().gen_range(
                        0,
                        Self::LONG_REORDERING_RANDOM_DELAY_BOUND_MILLIS,
                    );
                    let long_delay = Self::LONG_REORDERING_BASE_DELAY_MILLIS
                        + thread_rng().gen_range(0, 1 + long_delay_bound);
                    Self::delay_for_millis(long_delay).await;
                    // Falling through to send the result.
                }
            }
        }

        if let Err(_e) = rpc.reply_channel.send(reply) {
            // TODO(ditsing): log and do nothing.
        }
    }

    pub fn run_daemon() -> Arc<Mutex<Network>> {
        let mut network = Network::new();
        let rx = network
            .request_pipe
            .take()
            .expect("Newly created network should have a rx");

        let network = Arc::new(Mutex::new(network));

        let thread_pool = tokio::runtime::Builder::new()
            .threaded_scheduler()
            .core_threads(10)
            .max_threads(20)
            .thread_name("network")
            .enable_time()
            .build()
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
                    let mut locked_network = network
                        .lock()
                        .expect("Network mutex should not be poisoned");
                    if !locked_network.keep_running {
                        locked_network.is_running = false;
                        break;
                    }
                    locked_network.is_running = true;
                    stop_timer = Instant::now();
                }

                match rx.try_recv() {
                    Ok(rpc) => {
                        thread_pool
                            .spawn(Self::serve_rpc(network.clone(), rpc));
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
            is_running: false,
            rpc_count: std::cell::Cell::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::MutexGuard;

    use crate::test_utils::{junk_server::make_server, make_echo_rpc};
    use crate::Result;

    use super::*;

    fn make_network() -> Network {
        Network::new()
    }

    #[test]
    fn test_rpc_count_works() {
        let network = make_network();
        assert_eq!(0, network.get_total_rpc_count());

        network.increase_rpc_count();
        assert_eq!(1, network.get_total_rpc_count());
    }

    fn unlock<T>(network: &Arc<Mutex<T>>) -> MutexGuard<T> {
        network
            .lock()
            .expect("Network mutex should not be poisoned")
    }

    #[test]
    fn test_network_shutdown() {
        let network = Network::run_daemon();
        while !unlock(&network).is_running() {
            std::thread::sleep(Network::SHUTDOWN_DELAY)
        }
        let sender = {
            let mut network = unlock(&network);

            network.keep_running = false;

            network.request_bus.clone()
        };
        while unlock(&network).is_running() {
            std::thread::sleep(Network::SHUTDOWN_DELAY)
        }
        let (rpc, _) = make_echo_rpc("client", "server", &[]);
        let result = sender.send(rpc);
        assert!(
            result.is_err(),
            "Network is shutdown, requests should not be processed."
        );
    }

    #[test]
    fn test_proxy_rpc() -> Result<()> {
        let network = Network::run_daemon();
        let sender = {
            let mut network = unlock(&network);
            network.clients.insert(
                "test-client".into(),
                (true, "test-server".to_string()),
            );
            network.servers.insert("test-server".into(), make_server());
            network.request_bus.clone()
        };

        let (rpc, rx) =
            make_echo_rpc("test-client", "test-server", &[0x09, 0x00]);
        let result = sender.send(rpc);

        assert!(
            result.is_ok(),
            "Network is running, requests should be processed."
        );

        let reply = match futures::executor::block_on(rx) {
            Ok(reply) => reply,
            Err(e) => panic!("Future execution should not fail: {}", e),
        };
        match reply {
            Ok(reply) => assert_eq!(reply.as_ref(), &[0x00u8, 0x09u8]),
            Err(e) => panic!("Expecting echo message, got {}", e),
        }

        Ok(())
    }
}
