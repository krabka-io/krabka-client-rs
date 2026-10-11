# krabka-client-core

[![Crates.io](https://img.shields.io/crates/v/krabka-client-core.svg)](https://crates.io/crates/krabka-client-core)
[![Docs.rs](https://docs.rs/krabka-client-core/badge.svg)](https://docs.rs/krabka-client-core)
[![CI](https://github.com/krabka-io/krabka-client-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/krabka-io/krabka-client-rs/actions/workflows/ci.yml)

Connection management and typed request dispatch for Apache Kafka in Rust.

Part of [Krabka](https://github.com/krabka-io/krabka-client-rs), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

Krabka's higher-level admin, producer, consumer, and streams clients all share
`krabka-client-core` as their transport layer. It opens broker connections,
negotiates API versions, and routes requests by broker id. It sends typed Kafka
requests that `krabka-protocol` generates.

Use this crate to build a specialized Kafka client or test harness. Such a
client needs raw typed RPCs without producer batching, consumer group
management, or admin convenience wrappers.

## Capabilities

- Bootstrap DNS resolution and broker connection management.
- One pooled connection per broker id, plus direct `Connection` use.
- Correlation-id request/response multiplexing.
- API-version negotiation through `ApiVersions`.
- Typed `send` for any `ProtocolRequest` from `krabka-protocol`.
- Broker-routed request dispatch through `Client::broker`.
- Low-level partition fetch and offset-for-leader-epoch helpers.
- Client TLS and SASL configuration through `krabka-security`.

## Boundaries

This crate intentionally does not implement producer batching, idempotence,
transactions, consumer group coordination, commits, admin retries, or topology
runtime behavior. Those behaviors live in `krabka-client-producer`,
`krabka-client-consumer`, `krabka-client-admin`, and `krabka-client-streams`.

Connections use plaintext by default. Configure client security to change this.

## Install

```sh
cargo add krabka-client-core
cargo add krabka-protocol
```

For workspace development, use the path dependency from this repository.

## Usage

Send a typed Kafka protocol request over a negotiated client connection:

```rust,no_run
use krabka_client_core::Client;
use krabka_protocol::owned::api_versions_request::ApiVersionsRequest;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::builder()
    .bootstrap("127.0.0.1:9092")
    .client_id("metadata-prober")
    .build()
    .await?;

let response = client.send(ApiVersionsRequest::default()).await?;
println!("broker returned {} API keys", response.api_keys.len());

client.close();
# Ok(())
# }
```

## Cargo Features

- `mock` - exposes `MockBroker` support for downstream tests.

## Transport policy

`Client::builder` accepts DNS and request deadlines, socket setup timeout/max,
reconnect backoff/max, idle lifetime, DNS selection, socket buffers, dispatch
capacity, and frame limit. Direct `Connection` callers configure the same values
in `ConnectionOptions`; `validate()` checks them before I/O.

Request and setup deadlines must be positive and representable. Reconnect delays
and idle lifetime may be zero; maximum setup/reconnect values must cover their
initial values. Zero idle lifetime disables eviction. Socket buffers accept
positive whole bytes that fit `i32`, or `None` for OS defaults. Invalid settings
return `ClientError::InvalidConfig` before bootstrap or stream negotiation.

Defaults remain DNS 10s, request 30s, setup 10s/max 30s, reconnect 50ms/max 1s,
idle 9min, send 128KiB/receive 64KiB, dispatch64, and frame100MiB. Higher-level
clients may retain different role defaults, such as the admin five-minute idle
lifetime and producer 32KiB receive buffer.

## Documentation

- [API documentation](https://docs.rs/krabka-client-core)
- [krabka-client-rs repository](https://github.com/krabka-io/krabka-client-rs)
- [Kafka compatibility matrix](https://github.com/krabka-io/krabka-broker/blob/main/docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/krabka-io/krabka-client-rs/blob/main/NOTICE).
