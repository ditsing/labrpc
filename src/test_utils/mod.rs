use futures::channel::oneshot::Receiver;

use crate::{ReplyMessage, RequestMessage, Result, RpcOnWire};

pub(crate) mod junk_server;

pub(crate) fn make_aborting_rpc<C: Into<String>, S: Into<String>>(
    client: C,
    server: S,
) -> (RpcOnWire, Receiver<Result<ReplyMessage>>) {
    make_rpc(client, server, "aborting", &[])
}

pub(crate) fn make_echo_rpc<C: Into<String>, S: Into<String>>(
    client: C,
    server: S,
    data: &[u8],
) -> (RpcOnWire, Receiver<Result<ReplyMessage>>) {
    make_rpc(client, server, "echo", data)
}

pub(crate) fn make_rpc<C: Into<String>, S: Into<String>, M: Into<String>>(
    client: C,
    server: S,
    service_method: M,
    data: &[u8],
) -> (RpcOnWire, Receiver<Result<ReplyMessage>>) {
    let (tx, rx) = futures::channel::oneshot::channel();
    (
        RpcOnWire {
            client: client.into(),
            server: server.into(),
            service_method: service_method.into(),
            request: RequestMessage::copy_from_slice(data),
            reply_channel: tx,
        },
        rx,
    )
}
