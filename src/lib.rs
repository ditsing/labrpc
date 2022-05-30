#[cfg(feature = "tracing")]
mod tracing;

#[cfg(feature = "tracing")]
macro_rules! mark_trace {
    ($trace:expr, $name:ident) => {
        $crate::mark!($trace, $name)
    };
}

#[cfg(not(feature = "tracing"))]
macro_rules! mark_trace {
    ($trace:expr, $name:ident) => {};
}

mod client;
mod network;
mod server;

pub type Result<T> = std::io::Result<T>;
pub use client::Client;
pub use network::Network;
pub use server::Server;
#[cfg(feature = "tracing")]
pub use tracing::Trace;

// Messages passed on network.
struct RpcOnWire {
    client: ClientIdentifier,
    #[allow(dead_code)]
    server: ServerIdentifier,
    service_method: String,
    request: RequestMessage,

    reply_channel: futures::channel::oneshot::Sender<Result<ReplyMessage>>,
    #[cfg(feature = "tracing")]
    trace: tracing::TraceHolder,
}

pub type RequestMessage = bytes::Bytes;
pub type ReplyMessage = bytes::Bytes;

pub type ServerIdentifier = String;
pub type ClientIdentifier = String;

#[cfg(test)]
mod test_utils;
