use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvError, Sender};
use parking_lot::Mutex;
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
    request_bus: Sender<Option<RpcOnWire>>,

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
        self.request_bus
            .send(None)
            .expect("Sending RPCs should never fail.");
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

    pub fn set_enable_client<C: AsRef<str>>(&mut self, client: C, yes: bool) {
        if let Some(pair) = self.clients.get_mut(client.as_ref()) {
            pair.0 = yes;
        }
    }

    pub fn add_server<S: Into<ServerIdentifier>>(
        &mut self,
        server_name: S,
        server: Server,
    ) {
        self.servers.insert(server_name.into(), Arc::new(server));
    }

    pub fn remove_server<S: AsRef<str>>(&mut self, server_name: S) {
        let server = self.servers.remove(server_name.as_ref());
        if let Some(server) = server {
            server.interrupt();
        }
    }

    pub fn get_rpc_count<S: AsRef<str>>(
        &self,
        server_name: S,
    ) -> Option<usize> {
        self.servers
            .get(server_name.as_ref())
            .map(|s| s.rpc_count())
    }

    #[allow(clippy::ptr_arg)]
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

    const SHUTDOWN_DELAY: Duration = Duration::from_micros(100);

    async fn delay_for_millis(milli_seconds: u64) {
        tokio::time::sleep(Duration::from_millis(milli_seconds)).await;
    }

    async fn serve_rpc(network: Arc<Mutex<Self>>, rpc: RpcOnWire) {
        let (server_result, reliable, long_reordering, long_delays) = {
            let network = network.lock();
            network.increase_rpc_count();

            (
                network.dispatch(&rpc.client),
                network.reliable,
                network.long_reordering,
                network.long_delays,
            )
        };
        mark_trace!(rpc.trace, dispatched);

        // Random delay before sending requests to server.
        if !reliable {
            let minor_delay =
                thread_rng().gen_range(0..Self::MAX_MINOR_DELAY_MILLIS);
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
                mark_trace!(rpc.trace, served);
                return;
            }
        }

        let reply = match server_result {
            // Call the server.
            Ok(server) => {
                // Simulates the copy from network to server.
                let data = rpc.request.clone();
                // No need to set timeout. The RPCs are not supposed to block.
                mark_trace!(rpc.trace, before_serving);
                #[cfg(not(feature = "tracing"))]
                {
                    server.dispatch(rpc.service_method, data).await
                }
                #[cfg(feature = "tracing")]
                {
                    let res = server
                        .dispatch(rpc.service_method, data, rpc.trace.clone())
                        .await;
                    mark_trace!(rpc.trace, after_server_response);
                    res
                }
            }
            // If the server does not exist, return error after a random delay.
            Err(e) => {
                let long_delay_upper = if long_delays {
                    Self::MAX_LONG_DELAY_MILLIS
                } else {
                    Self::MAX_SHORT_DELAY_MILLIS
                };
                let long_delay =
                    rand::thread_rng().gen_range(0..long_delay_upper);
                Self::delay_for_millis(long_delay).await;
                Err(e)
            }
        };
        mark_trace!(rpc.trace, after_serving);

        let client = &rpc.client;
        let reply = reply.and_then(|reply| {
            // Fail the RPC if the client has been disconnected.
            network.lock().dispatch(client).map(|_| reply)
        });

        if reply.is_ok() {
            // Random drop again.
            if !reliable
                && thread_rng().gen_ratio(Self::DROP_RATE.0, Self::DROP_RATE.1)
            {
                // TODO(ditsing): add a real delay to random drop like above.
                let _ = rpc.reply_channel.send(Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "The network did not send respond in time.",
                )));
                mark_trace!(rpc.trace, served);
                return;
            } else if long_reordering {
                let should_reorder = thread_rng().gen_ratio(
                    Self::LONG_REORDERING_RATE.0,
                    Self::LONG_REORDERING_RATE.1,
                );
                if should_reorder {
                    let long_delay_bound = thread_rng().gen_range(
                        0..Self::LONG_REORDERING_RANDOM_DELAY_BOUND_MILLIS,
                    );
                    let long_delay = Self::LONG_REORDERING_BASE_DELAY_MILLIS
                        + thread_rng().gen_range(0..1 + long_delay_bound);
                    Self::delay_for_millis(long_delay).await;
                    // Falling through to send the result.
                }
            }
        }

        if let Err(_e) = rpc.reply_channel.send(reply) {
            // TODO(ditsing): log and do nothing.
        }
        mark_trace!(rpc.trace, served);
    }

    pub fn run_daemon() -> Arc<Mutex<Network>> {
        let (network, rx) = Network::new();

        // Using Mutex instead of RWLock, because most of the access are reads.
        let network = Arc::new(Mutex::new(network));

        // Using tokio instead of futures-rs, because we need timer futures.
        let thread_pool = tokio::runtime::Builder::new_multi_thread()
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
                    if !network.lock().keep_running {
                        break;
                    }
                    stop_timer = Instant::now();
                }

                match rx.recv() {
                    Ok(Some(rpc)) => {
                        mark_trace!(rpc.trace, dequeue);
                        thread_pool
                            .spawn(Self::serve_rpc(network.clone(), rpc));
                    }
                    Ok(None) => break,
                    // All senders have disconnected. This should never happen,
                    // since the network instance itself holds a sender.
                    Err(RecvError) => break,
                }
            }

            // Shutdown might leak outstanding tasks if timed-out.
            thread_pool.shutdown_timeout(Self::SHUTDOWN_DELAY);

            // rx is dropped here, all clients should get disconnected error
            // and stop sending messages.
            drop(rx);

            network.lock().stopped.store(true, Ordering::Release);
        });

        network
    }
}

impl Network {
    fn increase_rpc_count(&self) {
        self.rpc_count.set(self.rpc_count.get() + 1);
    }

    fn new() -> (Self, Receiver<Option<RpcOnWire>>) {
        // The channel has infinite buffer, could OOM the server if there are
        // too many pending RPCs to be served.
        let (tx, rx) = crossbeam_channel::unbounded();
        let network = Network {
            reliable: true,
            long_delays: false,
            long_reordering: false,
            clients: Default::default(),
            servers: Default::default(),
            request_bus: tx,
            keep_running: true,
            stopped: Default::default(),
            rpc_count: std::cell::Cell::new(0),
        };
        (network, rx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use parking_lot::MutexGuard;

    use crate::test_utils::{
        junk_server::{
            make_test_server, JunkRpcs, NON_CLIENT, NON_SERVER, TEST_CLIENT,
            TEST_SERVER,
        },
        make_aborting_rpc, make_echo_rpc,
    };
    use crate::{ReplyMessage, RequestMessage, Result};

    use super::*;

    #[test]
    fn test_rpc_count_works() {
        let (network, _) = Network::new();
        assert_eq!(0, network.get_total_rpc_count());

        network.increase_rpc_count();
        assert_eq!(1, network.get_total_rpc_count());
    }

    fn unlock<T>(network: &Arc<Mutex<T>>) -> MutexGuard<T> {
        network.lock()
    }

    #[test]
    fn test_network_shutdown() {
        let network = Network::run_daemon();
        let sender = {
            let mut network = unlock(&network);

            network.stop();

            network.request_bus.clone()
        };
        while !unlock(&network).stopped() {
            std::thread::sleep(Network::SHUTDOWN_DELAY)
        }
        let (rpc, _) = make_echo_rpc("client", "server", &[]);
        let result = sender.send(Some(rpc));
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
                .insert(TEST_SERVER.into(), Arc::new(make_test_server()));
            network.request_bus.clone()
        };

        let result = sender.send(Some(rpc));

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
    fn test_server_killed() -> Result<()> {
        let start_barrier = Arc::new(Barrier::new(2));
        let end_barrier = Arc::new(Barrier::new(2));
        let network = Network::run_daemon();
        let mut server = Server::make_server(TEST_SERVER);
        let start_barrier_clone = start_barrier.clone();
        let end_barrier_clone = end_barrier.clone();
        server.register_rpc_handler(
            "blocking".to_owned(),
            Box::new(move |args| {
                start_barrier_clone.wait();
                end_barrier_clone.wait();
                args.into()
            }),
        )?;

        unlock(&network).add_server(TEST_SERVER, server);
        let client = unlock(&network).make_client(TEST_CLIENT, TEST_SERVER);

        std::thread::spawn(move || {
            start_barrier.wait();
            unlock(&network).remove_server(TEST_SERVER);
            end_barrier.wait();
        });

        let reply = futures::executor::block_on(
            client.call_rpc("blocking".to_owned(), Default::default()),
        );

        let err = reply
            .expect_err("Client should receive error after server is killed");
        assert!(
            std::io::ErrorKind::ConnectionReset == err.kind()
                || std::io::ErrorKind::NotFound == err.kind()
        );

        Ok(())
    }

    fn make_network_and_client() -> (Arc<Mutex<Network>>, Client) {
        let network = Network::run_daemon();

        let server = make_test_server();
        unlock(&network).add_server(TEST_SERVER, server);

        let client = unlock(&network).make_client(TEST_CLIENT, TEST_SERVER);

        (network, client)
    }

    #[test]
    fn test_basic_functions() -> Result<()> {
        // Initialize
        let (network, client) = make_network_and_client();

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

    #[test]
    fn test_unreliable() -> Result<()> {
        let (network, _) = make_network_and_client();
        network.lock().set_reliable(false);
        const RPC_COUNT: usize = 1000;
        let mut handles = vec![];
        for i in 0..RPC_COUNT {
            let client = network
                .lock()
                .make_client(format!("{}-{}", TEST_CLIENT, i), TEST_SERVER);

            let handle = std::thread::spawn(move || {
                let reply = client.call_rpc(
                    JunkRpcs::Echo.name(),
                    RequestMessage::from_static(&[0x20, 0x17]),
                );
                futures::executor::block_on(reply)
            });
            handles.push(handle);
        }
        let mut success = 0;
        for handle in handles {
            let result = handle.join().expect("Thread join should not fail");
            if result.is_ok() {
                success += 1;
            }
        }
        let success = success as f64;
        assert!(
            success > RPC_COUNT as f64 * 0.75,
            "More than 15% RPC failed"
        );
        assert!(success < RPC_COUNT as f64 * 0.85, "Less than 5% RPC failed");
        Ok(())
    }

    #[test]
    #[ignore = "Large tests with many threads"]
    fn test_many_requests() {
        let now = Instant::now();

        let (network, _) = make_network_and_client();
        let barrier = Arc::new(Barrier::new(THREAD_COUNT + 1));
        const THREAD_COUNT: usize = 200;
        const RPC_COUNT: usize = 100;

        let mut handles = vec![];
        for i in 0..THREAD_COUNT {
            let network_ref = network.clone();
            let barrier_ref = barrier.clone();
            let handle = std::thread::spawn(move || {
                let client = unlock(&network_ref)
                    .make_client(format!("{}-{}", TEST_CLIENT, i), TEST_SERVER);
                // We should all create the client first.
                barrier_ref.wait();

                let mut results = vec![];
                for _ in 0..RPC_COUNT {
                    let reply = client.call_rpc(
                        JunkRpcs::Echo.name(),
                        RequestMessage::from_static(&[0x20, 0x17]),
                    );
                    results.push(reply);
                }
                for result in results {
                    futures::executor::block_on(result)
                        .expect("All futures should succeed");
                }
            });
            handles.push(handle);
        }
        barrier.wait();

        for handle in handles {
            handle.join().expect("All threads should succeed");
        }
        eprintln!("Many requests test took {:?}", now.elapsed());
    }

    // A typical request takes about 80ms.
    #[cfg(feature = "tracing")]
    #[test]
    fn test_tracing() -> Result<()> {
        let (_, client) = make_network_and_client();

        let (response, trace) = futures::executor::block_on(
            client.trace_rpc(JunkRpcs::Echo.name(), RequestMessage::new()),
        );
        assert!(response.is_ok());

        assert!(trace.assemble > Duration::from_secs(0));
        assert!(trace.enqueue > trace.assemble);
        // Dequeue can happen before enqueue, as they are on different threads.
        // assert!(trace.dequeue > trace.enqueue, "{:?}", trace);
        assert!(trace.dispatched > trace.dequeue);
        assert!(trace.before_serving > trace.dispatched);
        assert!(trace.after_serving > trace.before_serving);
        assert!(trace.served >= trace.after_serving, "{:?}", trace);
        // Response can be before serve, as they are on different threads.
        // assert!(trace.response >= trace.served);

        Ok(())
    }

    #[cfg(feature = "tracing")]
    #[test]
    #[ignore = "Large tests that runs for seconds"]
    fn test_dispatch_delay() -> Result<()> {
        let (_, client) = make_network_and_client();
        const ROUND: u32 = 100_000;

        let start = Instant::now();
        let mut dispatch_delay: Duration = Default::default();
        for _ in 0..ROUND {
            let (response, trace) = futures::executor::block_on(
                client.trace_rpc(JunkRpcs::Echo.name(), RequestMessage::new()),
            );
            assert!(response.is_ok());
            dispatch_delay += trace.response - trace.assemble;
        }
        eprintln!("The average RPC delay is {:?} ", dispatch_delay / ROUND);
        eprintln!(
            "The average client latency is {:?}",
            start.elapsed() / ROUND
        );

        Ok(())
    }
}
