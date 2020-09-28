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
    /// * Not connected: The client did not have a chance to send the request
    /// because the network is down.
    /// * Permission denied: The network does not allow the client to send
    /// requests.
    /// * Broken pipe: The network no longer allows the client to send requests.
    /// * Not found: The network could not find the target server.
    /// * Invalid input: The server could not find the service / method to call.
    /// * Connection reset: The server received the request, but decided to stop
    /// responding.
    /// * Connection aborted: The client will not receive a reply because the
    /// the connection is closed by the network.
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
                std::io::ErrorKind::NotConnected,
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

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{channel, Sender};

    use super::*;

    fn make_rpc_call(tx: Sender<RpcOnWire>) -> Result<ReplyMessage> {
        let client = Client {
            client: "C".into(),
            server: "S".into(),
            request_bus: tx,
        };

        let request = RequestMessage::from_static(&[0x17, 0x20]);
        futures::executor::block_on(client.call_rpc("hello".into(), request))
    }

    fn make_rpc_call_and_reply(
        reply: Result<ReplyMessage>,
    ) -> Result<ReplyMessage> {
        let (tx, rx) = channel();

        let handle = std::thread::spawn(move || make_rpc_call(tx));

        let rpc = rx.recv().expect("The request message should arrive");
        assert_eq!("C", &rpc.client);
        assert_eq!("S", &rpc.server);
        assert_eq!("hello", &rpc.service_method);
        assert_eq!(&[0x17, 0x20], rpc.request.as_ref());

        rpc.reply_channel
            .send(reply)
            .expect("The reply channel should not be closed");
        handle.join().expect("Rpc sending thread should succeed")
    }

    #[test]
    fn test_call_rpc() -> Result<()> {
        let data = &[0x11, 0x99];
        let reply =
            make_rpc_call_and_reply(Ok(ReplyMessage::from_static(data)))?;
        assert_eq!(data, reply.as_ref());

        Ok(())
    }

    #[test]
    fn test_call_rpc_remote_error() -> Result<()> {
        let reply = make_rpc_call_and_reply(Err(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "",
        )));

        if let Err(e) = reply {
            assert_eq!(std::io::ErrorKind::AddrInUse, e.kind());
        } else {
            panic!("Client should propagate remote error.")
        }

        Ok(())
    }

    #[test]
    fn test_call_rpc_remote_dropped() -> Result<()> {
        let (tx, rx) = channel();

        let handle = std::thread::spawn(move || make_rpc_call(tx));

        let rpc = rx.recv().expect("The request message should arrive");
        drop(rpc.reply_channel);

        let reply = handle.join().expect("Rpc sending thread should succeed");

        if let Err(e) = reply {
            assert_eq!(std::io::ErrorKind::ConnectionAborted, e.kind());
        } else {
            panic!(
                "Client should return error. Reply channel has been dropped."
            )
        }

        Ok(())
    }

    #[test]
    fn test_call_rpc_not_connected() -> Result<()> {
        let (tx, rx) = channel();
        {
            drop(rx);
        }

        let handle = std::thread::spawn(move || make_rpc_call(tx));
        let reply = handle.join().expect("Rpc sending thread should succeed");
        if let Err(e) = reply {
            assert_eq!(std::io::ErrorKind::NotConnected, e.kind());
        } else {
            panic!("Client should return error. request_bus has been dropped.")
        }
        Ok(())
    }
}
