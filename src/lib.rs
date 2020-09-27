extern crate bytes;
extern crate futures;
extern crate rand;
extern crate tokio_timer;

mod client;
mod network;
mod server;

type Result<T> = std::io::Result<T>;
pub use client::Client;
pub use network::Network;
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

type RequestMessage = bytes::Bytes;
type ReplyMessage = bytes::Bytes;

type ServerIdentifier = String;
type ClientIdentifier = String;

#[cfg(test)]
mod junk_server;