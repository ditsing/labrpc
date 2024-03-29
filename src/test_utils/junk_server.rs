use lazy_static::lazy_static;

use crate::Server;

pub const TEST_SERVER: &str = &"test-server";
pub const NON_SERVER: &str = &"non-server";

pub const TEST_CLIENT: &str = &"test-client";
pub const NON_CLIENT: &str = &"non-client";

pub enum JunkRpcs {
    Echo,
    Aborting,
    #[allow(dead_code)]
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

pub fn make_test_server() -> Server {
    let mut server = Server::make_server(TEST_SERVER);
    server
        .register_rpc_handler(
            JunkRpcs::Echo.name(),
            move |request: bytes::Bytes| {
                let mut reply = bytes::BytesMut::from(request.as_ref());
                reply.reverse();
                reply.freeze()
            },
        )
        .expect("Registering the first RPC handler should not fail");
    server
        .register_rpc_handler(
            JunkRpcs::Aborting.name(),
            Box::new(move |_| panic!("Aborting rpc...")),
        )
        .expect("Registering the second RPC handler should not fail");
    lazy_static! {
        static ref DEFAULT_RUNTIME: tokio::runtime::Runtime =
            tokio::runtime::Builder::new_multi_thread()
                .build()
                .expect("Build server default runtime should not fail");
    }
    server.use_pool(DEFAULT_RUNTIME.handle().clone());

    server
}
