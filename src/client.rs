use crate::{ClientIdentifier, RpcOnWire, ServerIdentifier};
use std::sync::mpsc::Sender;

// Client interface, used by the RPC client.
pub struct Client {
    client: ClientIdentifier,
    server: ServerIdentifier,

    request_bus: Sender<RpcOnWire>,
}
