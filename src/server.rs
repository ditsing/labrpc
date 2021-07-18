use std::collections::hash_map::Entry::Vacant;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;

use parking_lot::Mutex;

#[cfg(feature = "tracing")]
use crate::tracing::TraceHolder;
use crate::{ReplyMessage, RequestMessage, Result, ServerIdentifier};

pub type RpcHandler = dyn Fn(RequestMessage) -> ReplyMessage;

struct ServerState {
    rpc_handlers: std::collections::HashMap<String, Arc<RpcHandler>>,
    rpc_count: usize,
}

pub struct Server {
    name: String,
    state: Mutex<ServerState>,
    thread_pool: Option<tokio::runtime::Handle>,
    interrupt: tokio::sync::Notify,
}

impl Unpin for Server {}
// Server contains a immutable name, a mutex-protected state, and a thread pool.
// All of those 3 are `Send` and `Sync`.
unsafe impl Send for Server {}
unsafe impl Sync for Server {}

impl Server {
    pub(crate) async fn dispatch(
        self: Arc<Self>,
        service_method: String,
        data: RequestMessage,
        #[cfg(feature = "tracing")] trace: TraceHolder,
    ) -> Result<ReplyMessage> {
        let this = self.clone();
        mark_trace!(trace, before_server_scheduling);
        #[cfg(feature = "tracing")]
        let trace_clone = trace.clone();
        let runner = move || {
            let rpc_handler = {
                // Blocking on a mutex in a thread pool. Sounds horrible, but
                // in fact quite safe, given that the critical section is short.
                let mut state = self.state.lock();
                state.rpc_count += 1;
                state.rpc_handlers.get(&service_method).cloned()
            };
            mark_trace!(trace_clone, before_handling);
            let response = match rpc_handler {
                Some(rpc_handler) => {
                    Ok(catch_unwind(AssertUnwindSafe(|| rpc_handler(data))))
                }
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Method {} on server {} not found.",
                        service_method, self.name
                    ),
                )),
            };
            mark_trace!(trace_clone, after_handling);
            return match response {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(e)) => resume_unwind(e),
                Err(e) => Err(e),
            };
        };
        let thread_pool = this.thread_pool.as_ref().unwrap();
        // Using spawn() instead of spawn_blocking(), because the spawn() is
        // better at handling a large number of small workloads. Running
        // blocking code on async runner is fine, since all of the tasks we run
        // on this pool are blocking (for a limited time).
        let result = thread_pool.spawn(async { runner() });
        mark_trace!(trace, after_server_scheduling);
        let result = tokio::select! {
            result = result => Some(result),
            _ = this.interrupt.notified() => None,
        };
        let ret = match result {
            Some(Ok(ret)) => ret,
            Some(Err(_)) => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("Remote server {} cancelled the RPC.", this.name),
            )),
            None => {
                // Fail the RPC if the server has been terminated.
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "Network connection has been reset.".to_owned(),
                ))
            }
        };
        mark_trace!(trace, server_response);
        ret
    }

    pub fn register_rpc_handler(
        &mut self,
        service_method: String,
        rpc_handler: Box<RpcHandler>,
    ) -> Result<()> {
        let mut state = self.state.lock();
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
        self.state.lock().rpc_count
    }

    pub fn interrupt(&self) {
        self.interrupt.notify_waiters();
    }

    pub fn make_server<S: Into<ServerIdentifier>>(name: S) -> Self {
        let state = Mutex::new(ServerState {
            rpc_handlers: std::collections::HashMap::new(),
            rpc_count: 0,
        });
        Self {
            name: name.into(),
            state,
            thread_pool: None,
            interrupt: tokio::sync::Notify::new(),
        }
    }

    pub(crate) fn use_pool(&mut self, thread_pool: tokio::runtime::Handle) {
        self.thread_pool = Some(thread_pool);
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::junk_server::{
        make_test_server,
        JunkRpcs::{Aborting, Echo},
    };

    use super::*;

    fn rpc_handlers_len(server: &Server) -> usize {
        server.state.lock().rpc_handlers.len()
    }

    fn make_arc_test_server() -> Arc<Server> {
        Arc::new(make_test_server())
    }

    fn dispatch(
        server: Arc<Server>,
        service_method: String,
        data: RequestMessage,
    ) -> Result<ReplyMessage> {
        futures::executor::block_on(server.dispatch(
            service_method,
            data,
            #[cfg(feature = "tracing")]
            TraceHolder::make(),
        ))
    }

    #[test]
    fn test_register_rpc_handler() -> Result<()> {
        let server = make_test_server();

        assert_eq!(2, rpc_handlers_len(&server));
        Ok(())
    }

    #[test]
    fn test_register_rpc_handler_failure() -> Result<()> {
        let mut server = make_test_server();

        let result = server.register_rpc_handler(
            "echo".to_string(),
            Box::new(move |_| ReplyMessage::new()),
        );

        assert!(result.is_err());
        assert_eq!(2, rpc_handlers_len(&server));
        Ok(())
    }

    #[test]
    fn test_serve_rpc() -> Result<()> {
        let server = make_arc_test_server();

        let reply = dispatch(
            server,
            "echo".to_string(),
            RequestMessage::from_static(&[0x08, 0x07]),
        )?;

        assert_eq!(ReplyMessage::from_static(&[0x07, 0x08]), reply);
        Ok(())
    }

    #[test]
    fn test_rpc_not_found() -> Result<()> {
        let server = make_arc_test_server();

        let reply =
            dispatch(server, "acorn".to_string(), RequestMessage::new());
        match reply {
            Ok(_) => panic!("acorn service is not registered."),
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        }
        Ok(())
    }

    #[test]
    fn test_rpc_error() -> Result<()> {
        let server = make_arc_test_server();

        let reply = dispatch(server, Aborting.name(), RequestMessage::new());

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
    fn test_server_survives_30_rpc_errors() -> Result<()> {
        let server = make_arc_test_server();

        for _ in 0..30 {
            let server_clone = server.clone();
            let _ =
                dispatch(server_clone, Aborting.name(), RequestMessage::new());
        }

        let reply = dispatch(
            server,
            Echo.name(),
            RequestMessage::from_static(&[0x08, 0x07]),
        )?;

        assert_eq!(ReplyMessage::from_static(&[0x07, 0x08]), reply);

        Ok(())
    }
}
