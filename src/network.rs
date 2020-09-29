use std::collections::HashMap;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
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
    stopped: AtomicBool,

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

    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
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

    pub fn set_enable_client<C: AsRef<str> + Sized>(
        &mut self,
        client: C,
        yes: bool,
    ) {
        self.clients.get_mut(client.as_ref()).map(|pair| pair.0 = yes);
    }

    pub fn add_server<S: Into<ServerIdentifier>>(
        &mut self,
        server_name: S,
        server: Arc<Server>,
    ) {
        self.servers.insert(server_name.into(), server);
    }

    pub fn remove_server<S: AsRef<str> + Sized>(&mut self, server_name: &S) {
        self.servers.remove(server_name.as_ref());
    }

    pub fn get_rpc_count<S: AsRef<str> + Sized>(
        &self,
        server_name: S,
    ) -> Option<usize> {
        self.servers
            .get(server_name.as_ref())
            .map(|s| s.rpc_count())
    }

    fn dispatch(&self, client: &ClientIdentifier) -> Result<Arc<Server>> {
        let (enabled, server_name) =
            self.clients.get(client).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
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
                    let locked_network = network
                        .lock()
                        .expect("Network mutex should not be poisoned");
                    if !locked_network.keep_running {
                        break;
                    }
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

            // Shutdown might leak outstanding tasks if timed-out.
            thread_pool.shutdown_timeout(Self::SHUTDOWN_DELAY);

            // rx is dropped here, all clients should get disconnected error
            // and stop sending messages.
            drop(rx);

            network
                .lock()
                .expect("Network mutex should not be poisoned")
                .stopped
                .store(true, Ordering::Release);
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
            reliable: true,
            long_delays: false,
            long_reordering: false,
            clients: Default::default(),
            servers: Default::default(),
            request_bus: tx,
            request_pipe: Some(rx),
            keep_running: true,
            stopped: Default::default(),
            rpc_count: std::cell::Cell::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::MutexGuard;

    use crate::test_utils::{
        junk_server::{
            make_test_server, JunkRpcs, NON_CLIENT, NON_SERVER, TEST_CLIENT,
            TEST_SERVER,
        },
        make_aborting_rpc, make_echo_rpc,
    };
    use crate::{ReplyMessage, RequestMessage, Result};

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
        let sender = {
            let mut network = unlock(&network);

            network.keep_running = false;

            network.request_bus.clone()
        };
        while !unlock(&network).stopped() {
            std::thread::sleep(Network::SHUTDOWN_DELAY)
        }
        let (rpc, _) = make_echo_rpc("client", "server", &[]);
        let result = sender.send(rpc);
        assert!(
            result.is_err(),
            "Network is shutdown, requests should not be processed."
        );
    }

    fn send_rpc<C: Into<String>, S: Into<String>>(
        rpc: RpcOnWire,
        rx: futures::channel::oneshot::Receiver<Result<ReplyMessage>>,
        client: C,
        server: S,
        enabled: bool,
    ) -> Result<ReplyMessage> {
        let network = Network::run_daemon();
        let sender = {
            let mut network = unlock(&network);
            network
                .clients
                .insert(client.into(), (enabled, server.into()));
            network
                .servers
                .insert(TEST_SERVER.into(), make_test_server());
            network.request_bus.clone()
        };

        let result = sender.send(rpc);

        assert!(
            result.is_ok(),
            "Network is running, requests should be processed."
        );

        let reply = match futures::executor::block_on(rx) {
            Ok(reply) => reply,
            Err(e) => panic!("Future execution should not fail: {}", e),
        };

        reply
    }

    #[test]
    fn test_proxy_rpc() -> Result<()> {
        let (rpc, rx) =
            make_echo_rpc(TEST_CLIENT, TEST_SERVER, &[0x09u8, 0x00u8]);
        let reply = send_rpc(rpc, rx, TEST_CLIENT, TEST_SERVER, true);
        match reply {
            Ok(reply) => assert_eq!(reply.as_ref(), &[0x00u8, 0x09u8]),
            Err(e) => panic!("Expecting echo message, got {}", e),
        }

        Ok(())
    }

    #[test]
    fn test_proxy_rpc_server_error() -> Result<()> {
        let (rpc, rx) = make_aborting_rpc(TEST_CLIENT, TEST_SERVER);
        let reply = send_rpc(rpc, rx, TEST_CLIENT, TEST_SERVER, true);
        let err = reply.expect_err("Network should proxy server errors");
        assert_eq!(std::io::ErrorKind::ConnectionReset, err.kind());

        Ok(())
    }

    #[test]
    fn test_proxy_rpc_server_not_found() -> Result<()> {
        let (rpc, rx) = make_aborting_rpc(TEST_CLIENT, NON_SERVER);
        let reply = send_rpc(rpc, rx, TEST_CLIENT, NON_SERVER, true);
        let err = reply.expect_err("Network should check server in memory");
        assert_eq!(std::io::ErrorKind::NotFound, err.kind());

        Ok(())
    }

    #[test]
    fn test_proxy_rpc_client_disabled() -> Result<()> {
        let (rpc, rx) = make_aborting_rpc(TEST_CLIENT, TEST_SERVER);
        let reply = send_rpc(rpc, rx, TEST_CLIENT, TEST_SERVER, false);
        let err =
            reply.expect_err("Network should check if client is disabled");
        assert_eq!(std::io::ErrorKind::BrokenPipe, err.kind());

        Ok(())
    }

    #[test]
    fn test_proxy_rpc_no_such_client() -> Result<()> {
        let (rpc, rx) = make_aborting_rpc(NON_CLIENT, TEST_SERVER);
        let reply = send_rpc(rpc, rx, TEST_CLIENT, TEST_SERVER, true);
        let err = reply.expect_err("Network should check client names");
        assert_eq!(std::io::ErrorKind::PermissionDenied, err.kind());

        Ok(())
    }

    #[test]
    fn test_basic_functions() -> Result<()> {
        // Initialize
        let network = Network::run_daemon();

        let server = make_test_server();
        unlock(&network).add_server(TEST_SERVER, server);

        let client = unlock(&network).make_client(TEST_CLIENT, TEST_SERVER);

        assert_eq!(0, unlock(&network).get_total_rpc_count());

        let request = RequestMessage::from_static(&[0x17, 0x20]);
        let reply_data = &[0x20, 0x17];

        // Send first request.
        let reply = futures::executor::block_on(
            client.call_rpc(JunkRpcs::Echo.name(), request.clone()),
        )?;
        assert_eq!(reply_data, reply.as_ref());
        assert_eq!(1, unlock(&network).get_total_rpc_count());

        // Block the client.
        unlock(&network).set_enable_client(TEST_CLIENT, false);

        // Send second request.
        let reply = futures::executor::block_on(
            client.call_rpc(JunkRpcs::Echo.name(), request.clone()),
        );
        reply.expect_err("Client is blocked");
        assert_eq!(2, unlock(&network).get_total_rpc_count());
        assert_eq!(Some(1), unlock(&network).get_rpc_count(TEST_SERVER));
        assert_eq!(None, unlock(&network).get_rpc_count(NON_SERVER));

        // Unblock the client, then remove the server.
        unlock(&network).set_enable_client(TEST_CLIENT, true);
        unlock(&network).remove_server(&TEST_SERVER);

        // Send third request.
        let reply = futures::executor::block_on(
            client.call_rpc(JunkRpcs::Echo.name(), request.clone()),
        );
        reply.expect_err("Client is blocked");
        assert_eq!(3, unlock(&network).get_total_rpc_count());

        // Shutdown the network.
        unlock(&network).stop();

        while !unlock(&network).stopped() {
            std::thread::sleep(Duration::from_millis(10));
        }

        // Send forth request.
        let reply = futures::executor::block_on(
            client.call_rpc(JunkRpcs::Echo.name(), request.clone()),
        );
        reply.expect_err("Network is shutdown");
        assert_eq!(3, unlock(&network).get_total_rpc_count());

        // Done.
        Ok(())
    }
}
