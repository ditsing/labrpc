use crossbeam_channel::Sender;

#[cfg(feature = "tracing")]
use crate::tracing::{Trace, TraceHolder};
use crate::{
    ClientIdentifier, ReplyMessage, RequestMessage, Result, RpcOnWire,
    ServerIdentifier,
};

// Client interface, used by the RPC client.
pub struct Client {
    pub(crate) client: ClientIdentifier,
    pub(crate) server: ServerIdentifier,

    pub(crate) request_bus: Sender<Option<RpcOnWire>>,
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
        #[cfg(feature = "tracing")]
        {
            let trace = TraceHolder::make();
            self.trace_and_call_rpc(service_method, request, trace)
                .await
        }
        #[cfg(not(feature = "tracing"))]
        self.trace_and_call_rpc(service_method, request).await
    }

    async fn trace_and_call_rpc(
        &self,
        service_method: String,
        request: RequestMessage,
        #[cfg(feature = "tracing")] trace: TraceHolder,
    ) -> Result<ReplyMessage> {
        #[cfg(feature = "tracing")]
        let local_trace = trace.clone();

        let (tx, rx) = futures::channel::oneshot::channel();
        let rpc = RpcOnWire {
            client: self.client.clone(),
            server: self.server.clone(),
            service_method,
            request,
            reply_channel: tx,
            #[cfg(feature = "tracing")]
            trace,
        };

        mark_trace!(local_trace, assemble);
        self.request_bus.send(Some(rpc)).map_err(|e| {
            // The receiving end has been closed. Network connection is broken.
            std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                format!(
                    "Cannot send rpc, client {} is disconnected. {}",
                    self.client, e
                ),
            )
        })?;
        mark_trace!(local_trace, enqueue);

        #[allow(clippy::let_and_return)]
        let ret = rx.await.map_err(|e| {
            std::io::Error::new(
                // The network closed our connection. The server might not even
                // get a chance to see the request.
                std::io::ErrorKind::ConnectionAborted,
                format!("Network request is dropped: {}", e),
            )
        })?;
        mark_trace!(local_trace, response);
        ret
    }

    #[cfg(feature = "tracing")]
    pub async fn trace_rpc(
        &self,
        service_method: String,
        request: RequestMessage,
    ) -> (Result<ReplyMessage>, Trace) {
        let trace = TraceHolder::make();
        let local_trace = trace.clone();

        let response = self
            .trace_and_call_rpc(service_method, request, trace)
            .await;

        (response, local_trace.extract())
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::{unbounded, Sender};

    use super::*;

    fn make_rpc_call(tx: Sender<Option<RpcOnWire>>) -> Result<ReplyMessage> {
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
        let (tx, rx) = unbounded();

        let handle = std::thread::spawn(move || make_rpc_call(tx));

        let rpc = rx
            .recv()
            .expect("The request message should arrive")
            .expect("The request message should not be null");
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
        let (tx, rx) = unbounded();

        let handle = std::thread::spawn(move || make_rpc_call(tx));

        let rpc = rx
            .recv()
            .expect("The request message should arrive")
            .expect("The request message should not be null");
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
        let (tx, rx) = unbounded();
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

    async fn make_rpc(client: Client) -> Result<ReplyMessage> {
        let request = RequestMessage::from_static(&[0x17, 0x20]);
        client.call_rpc("hello".into(), request).await
    }

    #[test]
    fn test_call_across_threads() -> Result<()> {
        let (tx, rx) = unbounded();
        let rpc_future = {
            let client = Client {
                client: "C".into(),
                server: "S".into(),
                request_bus: tx,
            };
            make_rpc(client)
        };
        std::thread::spawn(move || {
            let _ = futures::executor::block_on(rpc_future);
        });
        let rpc = rx
            .recv()
            .expect("The request message should arrive")
            .expect("The request message should not be null");
        rpc.reply_channel
            .send(Ok(Default::default()))
            .expect("The reply channel should not be closed");
        Ok(())
    }
}
