use crate::{ReplyMessage, RequestMessage, Result};
use std::collections::hash_map::Entry::Vacant;
use std::sync::Arc;

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
                    .expect("The server state mutex should not be poisoned.");
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
            .expect("The server state mutex should not be poisoned.");
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

    pub fn make_server(name: String) -> Self {
        let state = std::sync::Mutex::new(ServerState {
            rpc_handlers: std::collections::HashMap::new(),
            rpc_count: std::cell::Cell::new(0),
        });
        let thread_pool = futures::executor::ThreadPool::builder()
            .name_prefix(name.clone())
            .pool_size(Self::THREAD_POOL_SIZE)
            .create()
            .expect("Creating thread pools should not fail.");
        Self {
            name,
            state,
            thread_pool,
        }
    }
}
