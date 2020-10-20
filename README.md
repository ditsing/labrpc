# An RPC framework for testing: labrpc

This is a line-by-line translation of [labrpc](https://godoc.org/github.com/AlexShtarbev/mit_ds/labrpc) from Go to Rust.
`labrpc` helps test a distributed system in a controlled environment. It can simulate networks that has long delays,
drops packets randomly, or sends RPC responses in random order. Peers connected to the network can be added and removed
on the fly, or disabled and re-enabled individually.

This Rust version utilizes the `async-await` feature, making the implementation safe, reliable and efficient at the same
time.

## Usage
`labrpc` allows creating a network that passes RPC calls. RPCs servers can be registered in the network, with a unique
name. Clients can connect to servers via server names. Each client is identified by its name, so that they can
be disabled if and when needed.

A typical use case is as follows.

```Rust
    // Start the network.
    let network = labrpc::Network::run_daemon();
    // Make it unreliable.
    network.lock().set_reliable(true);

    // Make an RPC server.
    let server = { ... };
    // Register the server in the network, call it "echo-server".
    network.lock().add_server(&"echo-server", server);
    
    // Make a connection to the above server.
    let client = network.lock().make_client(&"echo-client", &"echo-server");
    // Build and send a request.
    let request = { ... };
    // Await for the reply.
    let reply = client.call_rpc("EchoService.Echo", request).await?;

    // Echo server should echo.
    assert_eq!(request.get_message(), reply.get_message());

    // Disable the client we created above.
    network.lock().set_enable_client(&"echo-client", false);

    // Now we should get errors.
    let result = client.call_rpc("EchoServer.Echo").await;
    assert!(resut.is_err());
```

## Implementation
Behind the scenes, all clients send requests through a shared request queue. There is a dedicated thread taking requests
from the queue and delegating the request to a set of workers. Each worker looks up the client, checks ACLs, finds the
right server, and calls the server. Workers also decides if the request should be dropped or delayed.

## Latency
On average, each RPC takes about 60ms to travel through the network.

For unknown reasons, this implementation is only half as fast as the Go version, in the scenario where there is only one
client which sends millions of requests. Adding more clients that behaves similarly does not decrease performance.