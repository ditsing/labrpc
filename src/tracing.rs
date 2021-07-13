#![cfg(feature = "tracing")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

#[derive(Clone, Debug)]
pub struct Trace {
    /// When the trace is created by the client.
    pub created_at: Instant,
    /// The delay of when the request is assembled.
    pub assemble: Duration,
    /// The delay of when the request is sent to the network.
    pub enqueue: Duration,
    /// The delay of when the request is received by the network.
    pub dequeue: Duration,
    /// The delay of when the request is sent to the server.
    pub dispatched: Duration,
    /// The delay of when the request is about to be processed by the server.
    pub before_serving: Duration,
    pub before_server_scheduling: Duration,
    pub after_server_scheduling: Duration,
    pub before_handling: Duration,
    pub after_handling: Duration,
    pub server_response: Duration,
    pub after_server_response: Duration,
    /// The delay of when the request is served by the server.
    pub after_serving: Duration,
    /// The delay of when the network proxies the response from the server.
    pub served: Duration,
    /// The delay of when the client receives the response from the network.
    pub response: Duration,
}

#[macro_export]
macro_rules! mark {
    ($trace:expr, $name:ident) => {{
        let mut trace = $trace.inner.lock();
        trace.$name = std::time::Instant::now() - trace.created_at;
    }};
}

impl Trace {
    pub(crate) fn start() -> Trace {
        Self {
            created_at: Instant::now(),
            assemble: Default::default(),
            enqueue: Default::default(),
            dequeue: Default::default(),
            dispatched: Default::default(),
            before_serving: Default::default(),
            before_server_scheduling: Default::default(),
            after_server_scheduling: Default::default(),
            before_handling: Default::default(),
            after_handling: Default::default(),
            server_response: Default::default(),
            after_server_response: Default::default(),
            after_serving: Default::default(),
            served: Default::default(),
            response: Default::default(),
        }
    }
}

// Clone is required because the client side code need to mark a trace
// after sending an RPC.
#[derive(Clone)]
pub struct TraceHolder {
    pub(crate) inner: Arc<Mutex<Trace>>,
}

impl TraceHolder {
    pub(crate) fn make() -> Self {
        let inner = Trace::start();
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub(crate) fn extract(&self) -> Trace {
        // Another way to do it would be to unwrap the Arc.
        let mut trace = self.inner.lock().clone();
        if trace.served.as_nanos() == 0 {
            trace.served = trace.response
        }
        trace
    }
}
