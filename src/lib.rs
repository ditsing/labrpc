extern crate bytes;
extern crate futures;

mod client;
mod network;
mod server;

type Result<T> = std::io::Result<T>;
pub use client::Client;
pub use server::Server;
pub use network::Network;

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
