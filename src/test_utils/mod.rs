use crate::{ReplyMessage, RequestMessage, Result, RpcOnWire};
use futures::channel::oneshot::Receiver;

pub(crate) mod junk_server;

pub(crate) fn make_echo_rpc<C: Into<String>, S: Into<String>>(
    client: C,
    server: S,
) -> (RpcOnWire, Receiver<Result<ReplyMessage>>) {
    let (tx, rx) = futures::channel::oneshot::channel();
    (
        RpcOnWire {
            client: client.into(),
            server: server.into(),
            service_method: "echo".into(),
            request: RequestMessage::new(),
            reply_channel: tx,
        },
        rx,
    )
}
