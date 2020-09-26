extern crate bytes;
extern crate futures;

mod server;

use crate::server::Server;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

type Result<T> = std::io::Result<T>;

// Messages passed on network.
struct RequestMessage<'a> {
    service_method: String,
    arg: &'a [u8],
}

type ReplyMessage = Bytes;

type ServerIdentifier = String;
type ClientIdentifier = String;

// Client interface, used by the RPC client.
struct Client {
    client: ClientIdentifier,
    server: ServerIdentifier,
    // Closing signal,
}

struct Network {
    // Need a lock field
    // Settings.
    reliable: bool,
    long_delays: bool,
    long_reordering: bool,

    // Clients
    clients: HashMap<ClientIdentifier, (bool, ServerIdentifier)>,
    servers: HashMap<ServerIdentifier, Arc<Server>>,

    // Closing signal.

    // RPC Counter, using Cell for interior mutability.
    rpc_count: std::cell::Cell<usize>,
}

impl Network {
    pub fn cleanup(self) {
        unimplemented!()
    }

    pub fn set_reliable(&mut self, yes: bool) {
        self.reliable = yes
    }

    pub fn set_long_reordering(&mut self, yes: bool) {
        self.long_reordering = yes
    }

    pub fn set_long_delays(&mut self, yes: bool) {
        self.long_delays = yes
    }

    pub fn make_connection(_server_name: ServerIdentifier) -> Client {
        unimplemented!()
    }

    pub async fn dispatch(
        &self,
        client: ClientIdentifier,
        request: RequestMessage<'_>,
    ) -> Result<ReplyMessage> {
        self.increase_rpc_count();
        let (enabled, server_name) =
            self.clients.get(&client).ok_or_else(|| {
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

        // Simulates the copy from network to server.
        let data = Bytes::copy_from_slice(request.arg);
        server.clone().dispatch(request.service_method, data).await
    }

    pub fn get_total_rpc_count(&self) -> usize {
        self.rpc_count.get()
    }
}

impl Network {
    fn increase_rpc_count(&self) {
        self.rpc_count.set(self.rpc_count.get() + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_network() -> Network {
        Network {
            reliable: false,
            long_delays: false,
            long_reordering: false,
            clients: Default::default(),
            servers: Default::default(),
            rpc_count: std::cell::Cell::new(0),
        }
    }

    #[test]
    fn rpc_count_works() {
        let network = make_network();
        assert_eq!(0, network.get_total_rpc_count());

        network.increase_rpc_count();
        assert_eq!(1, network.get_total_rpc_count());
    }
}
