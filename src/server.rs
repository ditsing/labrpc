use std::collections::hash_map::Entry::Vacant;
use std::sync::Arc;

use crate::{ReplyMessage, RequestMessage, Result};

pub trait RpcHandler {
    // Note this method is not async.
    fn call(&self, data: RequestMessage) -> ReplyMessage;
}

struct ServerState {
    rpc_handlers: std::collections::HashMap<String, Arc<Box<dyn RpcHandler>>>,
    rpc_count: std::cell::Cell<usize>,
}

pub struct Server {
    name: String,
    state: std::sync::Mutex<ServerState>,
    thread_pool: futures::executor::ThreadPool,
}

impl Unpin for Server {}
// Server contains a immutable name, a mutex-protected state, and a thread pool.
// All of those 3 are `Send` and `Sync`.
unsafe impl Send for Server {}
unsafe impl Sync for Server {}

impl Server {
    const THREAD_POOL_SIZE: usize = 4;
    pub async fn dispatch(
        self: Arc<Self>,
        service_method: String,
        data: RequestMessage,
    ) -> Result<ReplyMessage> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let this = self.clone();
        this.thread_pool.spawn_ok(async move {
            let rpc_handler = {
                // Blocking on a mutex in a thread pool. Sounds horrible, but
                // in fact quite safe, given that the critical section is short.
                let state = self
                    .state
                    .lock()
                    .expect("The server state mutex should not be poisoned");
                state.rpc_count.set(state.rpc_count.get() + 1);
                state.rpc_handlers.get(&service_method).map(|r| r.clone())
            };
            let response = match rpc_handler {
                Some(rpc_handler) => Ok(rpc_handler.call(data)),
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Method {} on server {} not found.",
                        service_method, self.name
                    ),
                )),
            };
            if let Err(_) = tx.send(response) {
                // Receiving end is dropped. Never mind.
                // Do nothing.
            }
        });
        rx.await.map_err(|_e| {
            std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("Remote server {} cancelled the RPC.", this.name),
            )
        })?
    }

    pub fn register_rpc_handler(
        &mut self,
        service_method: String,
        rpc_handler: Box<dyn RpcHandler>,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .expect("The server state mutex should not be poisoned");
        let debug_service_method = service_method.clone();
        if let Vacant(vacant) = state.rpc_handlers.entry(service_method) {
            vacant.insert(Arc::new(rpc_handler));
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "Service method {} already exists in server {}.",
                    debug_service_method, self.name
                ),
            ))
        }
    }

    pub fn rpc_count(&self) -> usize {
        self.state
            .lock()
            .expect("The server state mutex should not be poisoned")
            .rpc_count
            .get()
    }

    pub fn make_server(name: String) -> Self {
        let state = std::sync::Mutex::new(ServerState {
            rpc_handlers: std::collections::HashMap::new(),
            rpc_count: std::cell::Cell::new(0),
        });
        let thread_pool = futures::executor::ThreadPool::builder()
            .name_prefix(name.clone())
            .pool_size(Self::THREAD_POOL_SIZE)
            .create()
            .expect("Creating thread pools should not fail");
        Self {
            name,
            state,
            thread_pool,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::junk_server::{
        make_test_server, EchoRpcHandler,
        JunkRpcs::{Aborting, Echo},
    };

    use super::*;

    fn rpc_handlers_len(server: &Server) -> usize {
        server
            .state
            .lock()
            .expect("The server state mutex should not be poisoned.")
            .rpc_handlers
            .len()
    }

    #[test]
    fn test_register_rpc_handler() -> Result<()> {
        let server = make_test_server();

        assert_eq!(2, rpc_handlers_len(server.as_ref()));
        Ok(())
    }

    #[test]
    fn test_register_rpc_handler_failure() -> Result<()> {
        let mut server = make_test_server();
        let server = std::sync::Arc::get_mut(&mut server)
            .expect("Server should only be held by the current thread");

        let result = server.register_rpc_handler(
            "echo".to_string(),
            Box::new(EchoRpcHandler {}),
        );

        assert!(result.is_err());
        assert_eq!(2, rpc_handlers_len(server));
        Ok(())
    }

    #[test]
    fn test_serve_rpc() -> Result<()> {
        let server = make_test_server();

        let reply = server.dispatch(
            "echo".to_string(),
            RequestMessage::from_static(&[0x08, 0x07]),
        );
        let result = futures::executor::block_on(reply)?;

        assert_eq!(ReplyMessage::from_static(&[0x07, 0x08]), result);
        Ok(())
    }

    #[test]
    fn test_rpc_not_found() -> Result<()> {
        let server = make_test_server();

        let reply = server.dispatch("acorn".to_string(), RequestMessage::new());
        match futures::executor::block_on(reply) {
            Ok(_) => panic!("acorn service is not registered."),
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        }
        Ok(())
    }

    #[test]
    fn test_rpc_error() -> Result<()> {
        let server = make_test_server();

        let reply = futures::executor::block_on(
            server.dispatch(Aborting.name(), RequestMessage::new()),
        );

        assert_eq!(
            reply
                .err()
                .expect("Aborting RPC should return error")
                .kind(),
            std::io::ErrorKind::ConnectionReset,
        );

        Ok(())
    }

    #[test]
    fn test_server_survives_3_rpc_errors() -> Result<()> {
        let server = make_test_server();

        // TODO(ditsing): server hangs after the 4th RPC error.
        for _ in 0..3 {
            let server_clone = server.clone();
            let _ = futures::executor::block_on(
                server_clone.dispatch(Aborting.name(), RequestMessage::new()),
            );
        }

        let reply = server
            .dispatch(Echo.name(), RequestMessage::from_static(&[0x08, 0x07]));
        let result = futures::executor::block_on(reply)?;

        assert_eq!(ReplyMessage::from_static(&[0x07, 0x08]), result);

        Ok(())
    }
}
