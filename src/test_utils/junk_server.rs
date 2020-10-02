use crate::{server::RpcHandler, ReplyMessage, RequestMessage, Server};

pub const TEST_SERVER: &str = &"test-server";
pub const NON_SERVER: &str = &"non-server";

pub const TEST_CLIENT: &str = &"test-client";
pub const NON_CLIENT: &str = &"non-client";

pub enum JunkRpcs {
    Echo,
    Aborting,
    Woods,
}

impl JunkRpcs {
    pub fn name(&self) -> String {
        match *self {
            Self::Echo => "echo",
            Self::Aborting => "aborting",
            Self::Woods => "woods",
        }
        .into()
    }
}

pub struct EchoRpcHandler {}

impl RpcHandler for EchoRpcHandler {
    fn call(&self, request: RequestMessage) -> ReplyMessage {
        let mut reply = bytes::BytesMut::from(request.as_ref());
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

pub fn make_test_server() -> Server {
    let mut server = Server::make_server(TEST_SERVER);
    server
        .register_rpc_handler(
            JunkRpcs::Echo.name(),
            Box::new(EchoRpcHandler {}),
        )
        .expect("Registering the first RPC handler should not fail");
    server
        .register_rpc_handler(
            JunkRpcs::Aborting.name(),
            Box::new(AbortingRpcHandler {}),
        )
        .expect("Registering the second RPC handler should not fail");
    server
}
