#![allow(dead_code)]

extern crate bytes;
extern crate futures;

mod client;
mod network;
mod server;

type Result<T> = std::io::Result<T>;
pub use client::Client;
pub use server::Server;

// Messages passed on network.
struct RpcOnWire {
    client: ClientIdentifier,
    server: ServerIdentifier,
    service_method: String,
    request: RequestMessage,

    reply_channel: std::sync::mpsc::Sender<Result<ReplyMessage>>,
}

type RequestMessage = bytes::Bytes;
type ReplyMessage = bytes::Bytes;

type ServerIdentifier = String;
type ClientIdentifier = String;
