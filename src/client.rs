use std::sync::mpsc::Sender;

use crate::{
    ClientIdentifier, ReplyMessage, RequestMessage, Result, RpcOnWire,
    ServerIdentifier,
};

// Client interface, used by the RPC client.
pub struct Client {
    pub(crate) client: ClientIdentifier,
    pub(crate) server: ServerIdentifier,

    pub(crate) request_bus: Sender<RpcOnWire>,
}

impl Client {
    /// Error type and meaning
    /// * Connection aborted: The client did not have a chance to send the
    /// request, or will not receive a reply because the network is down.
    /// * Not connected: The network does not allow the client to send requests.
    /// * Broken pipe: The network no longer allows the client to send requests.
    /// * Not found: The network could not find the target server.
    /// * Invalid input: The server could not find the service / method to call.
    /// * Connection reset: The server received the request, but decided to stop
    /// responding.
    pub async fn call_rpc(
        &self,
        service_method: String,
        request: RequestMessage,
    ) -> Result<ReplyMessage> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let rpc = RpcOnWire {
            client: self.client.clone(),
            server: self.server.clone(),
            service_method,
            request,
            reply_channel: tx,
        };

        self.request_bus.send(rpc).map_err(|e| {
            // The receiving end has been closed. Network connection is broken.
            std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!(
                    "Cannot send rpc, client {} is disconnected. {}",
                    self.client.clone(),
                    e
                ),
            )
        })?;

        rx.await.map_err(|e| {
            std::io::Error::new(
                // The network closed our connection. The server might not even
                // get a chance to see the request.
                std::io::ErrorKind::ConnectionAborted,
                format!("Network request is dropped: {}", e),
            )
        })?
    }
}
