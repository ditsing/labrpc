use crate::{server::RpcHandler, ReplyMessage, RequestMessage, Server};
use std::sync::Arc;
use bytes::BytesMut;

pub struct EchoRpcHandler {}

impl RpcHandler for EchoRpcHandler {
    fn call(&self, request: RequestMessage) -> ReplyMessage {
        let mut reply = BytesMut::from(request.as_ref());
        reply.reverse();
        reply.freeze()
    }
}

pub struct AbortingRpcHandler {}

impl RpcHandler for AbortingRpcHandler {
    fn call(&self, _data: RequestMessage) -> ReplyMessage {
        panic!("Aborting rpc...")
    }
}

pub fn make_server() -> Arc<Server> {
    let mut server = Server::make_server("test-server".to_string());
    server
        .register_rpc_handler("echo".to_string(), Box::new(EchoRpcHandler {}))
        .expect("Registering the first RPC handler should not fail.");
    server
        .register_rpc_handler(
            "aborting".to_string(),
            Box::new(AbortingRpcHandler {}),
        )
        .expect("Registering the second RPC handler should not fail.");
    Arc::new(server)
}
