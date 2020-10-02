extern crate bytes;
extern crate futures;
extern crate rand;
extern crate tokio;

mod client;
mod network;
mod server;

pub type Result<T> = std::io::Result<T>;
pub use client::Client;
pub use network::Network;
pub use server::RpcHandler;
pub use server::Server;

// Messages passed on network.
struct RpcOnWire {
    client: ClientIdentifier,
    #[allow(dead_code)]
    server: ServerIdentifier,
    service_method: String,
    request: RequestMessage,

    reply_channel: futures::channel::oneshot::Sender<Result<ReplyMessage>>,
}

pub type RequestMessage = bytes::Bytes;
pub type ReplyMessage = bytes::Bytes;

pub type ServerIdentifier = String;
pub type ClientIdentifier = String;

#[cfg(test)]
mod test_utils;
