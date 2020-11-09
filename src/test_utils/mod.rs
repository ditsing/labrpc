use futures::channel::oneshot::Receiver;

use crate::{ReplyMessage, RequestMessage, Result, RpcOnWire};

pub(crate) mod junk_server;

pub(crate) fn make_aborting_rpc<C: Into<String>, S: Into<String>>(
    client: C,
    server: S,
) -> (RpcOnWire, Receiver<Result<ReplyMessage>>) {
    make_rpc(client, server, junk_server::JunkRpcs::Aborting, &[])
}

pub(crate) fn make_echo_rpc<C: Into<String>, S: Into<String>>(
    client: C,
    server: S,
    data: &[u8],
) -> (RpcOnWire, Receiver<Result<ReplyMessage>>) {
    make_rpc(client, server, junk_server::JunkRpcs::Echo, data)
}

pub(crate) fn make_rpc<C: Into<String>, S: Into<String>>(
    client: C,
    server: S,
    service_method: junk_server::JunkRpcs,
    data: &[u8],
) -> (RpcOnWire, Receiver<Result<ReplyMessage>>) {
    let (tx, rx) = futures::channel::oneshot::channel();
    (
        RpcOnWire {
            client: client.into(),
            server: server.into(),
            service_method: service_method.name(),
            request: RequestMessage::copy_from_slice(data),
            reply_channel: tx,
            #[cfg(feature = "tracing")]
            trace: crate::tracing::TraceHolder::make(),
        },
        rx,
    )
}
