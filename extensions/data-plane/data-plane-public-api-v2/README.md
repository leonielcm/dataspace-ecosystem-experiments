# Data Plane Public API v2

The HTTP proxy of the data plane, on both sides of a PULL transfer:

- **Provider public API** (`/api/public`, `DataPlanePublicApiV2Extension`). It takes the EDR token as `Authorization`, checks it, and proxies the request to the asset's `HttpData` source.
- **Consumer data API** (`/api/data`, `DataPlaneProxyApiExtension`). It takes the contract agreement id as `Authorization`, looks up the matching EDR in the consumer's EDR store, and proxies the request to the provider public API with the EDR token.

## Table of Contents

- [Relaying headers and response status](#relaying-headers-and-response-status)
- [Publishing an MCP server as an asset](#publishing-an-mcp-server-as-an-asset)
- [Settings](#settings)
- [How it works](#how-it-works)
- [Limitations](#limitations)
- [Testing](#testing)

## Relaying headers and response status

By default, the proxy relays only the method, path, query, body and content type of a request. It answers `200` with the body of the source, and turns any non-2xx answer of the source into a `500`. That is enough for a REST API. It is not enough for protocols that rely on headers and statuses, such as MCP (Model Context Protocol) over Streamable HTTP:

- the client sends `Accept: application/json, text/event-stream` and `MCP-Protocol-Version`;
- a stateful server returns `Mcp-Session-Id` on `initialize`, and the client sends it back on every later request;
- the server answers `202` to notifications and `405` when it does not offer the SSE stream, and clients rely on those statuses.

An asset opts in with the data address property `proxyHeaders=true`. For such an asset, the proxy relays, on both hops:

- **to the source:** the request headers of an allow-list (`edc.dataplane.proxy.forwarded.headers`);
- **back to the client:** the status of the source and the response headers of a second allow-list (`edc.dataplane.proxy.forwarded.response.headers`).

Assets without `proxyHeaders` behave exactly as before.

Some headers are never relayed, whatever the configuration says: `Authorization`, `Proxy-Authorization`, `Proxy-Authenticate`, `Cookie`, `Set-Cookie`, `Contract-Id`, `Host`, `Content-Type`, `Content-Length`, hop-by-hop headers and the internal `X-Dse-Proxy-Relayed` marker. Listing one of them in a setting makes the runtime fail at startup. Headers set by the provider on the data address itself (`header:*` additional headers, `authKey`) always win over relayed ones.

## Publishing an MCP server as an asset

| Data address property | Value | Why |
|---|---|---|
| `type` | `HttpData` | |
| `baseUrl` | the MCP endpoint, e.g. `https://mcp.example.com/mcp` | an MCP server has a single endpoint |
| `proxyMethod` | `true` | messages are POSTed, the SSE stream is a GET, the session is closed with a DELETE |
| `proxyBody` | `true` | JSON-RPC messages travel in the body |
| `proxyHeaders` | `true` | session and protocol version headers, response statuses |
| `proxyPath`, `proxyQueryParams` | leave unset | MCP uses neither; leaving them unset keeps the client on the MCP endpoint |

Through the management API:

```json
{
  "@context": { "@vocab": "https://w3id.org/edc/v0.0.1/ns/" },
  "@id": "mcp-server",
  "properties": { "description": "MCP server" },
  "dataAddress": {
    "type": "HttpData",
    "baseUrl": "https://mcp.example.com/mcp",
    "proxyMethod": "true",
    "proxyBody": "true",
    "proxyHeaders": "true"
  }
}
```

Policies, contract definitions, negotiation and transfer (`HttpData-PULL`) are unchanged. The consumer points its MCP client at its data API, `https://<host>/<consumer>/dp/data`, with the agreement id as `Authorization`. With the official MCP SDKs, that means a custom request header on the Streamable HTTP transport.

A contract grants access to the whole MCP server, all its tools and resources, just as a REST asset with `proxyPath=true` grants access to the whole API.

## Settings

| Key | Default | Meaning |
|---|---|---|
| `edc.dataplane.proxy.forwarded.headers` | `Accept,MCP-Protocol-Version,Mcp-Session-Id,Last-Event-ID` | request headers relayed to the source, case-insensitive |
| `edc.dataplane.proxy.forwarded.response.headers` | `Mcp-Session-Id` | response headers of the source relayed to the client, case-insensitive |

## How it works

All the classes below are in the `headers` package, except `RelayingStreamingDataSink`, which is in `controller`.

- `ForwardedHeadersExtension` reads both allow-lists. It registers `ForwardedHeadersParamsDecorator` and `ProxyHttpDataSourceFactory`, and provides `ForwardedHeaders` to both controllers.
- **Request.** `DataFlowRequestSupplier` copies the allowed request headers into the `DataFlowStartMessage`, as `forwarded-header:<name>` properties. `ForwardedHeadersParamsDecorator` applies them to the outgoing request when the address has `proxyHeaders=true`.
- **Source.** `DataFlowRequestSupplier` gives an address with `proxyHeaders=true` the `DseProxyHttpData` type, so that `ProxyHttpDataSource` serves it instead of the EDC `HttpDataSource`. Unlike the EDC source, it does not turn non-2xx answers into failures. It returns a `ProxyHttpPart` that carries the status and the allowed response headers along with the body.
- **Response.** `RelayingStreamingDataSink` streams the body to the client like the EDC `AsyncStreamingDataSink`, and applies the status and headers of a `ProxyHttpPart`. Any other part is answered `200`, as before.
- **Between the two hops.** The consumer data API always sets `proxyHeaders=true` on its address to the provider public API, because it does not know the provider asset's setting. To keep the existing behaviour for other assets, the provider marks every answer it relays from the source with `X-Dse-Proxy-Relayed: true`.
  - The consumer relays a non-2xx answer of the provider only when that marker is present. The marker itself is never relayed to the client.
  - Errors raised by the provider connector itself (rejected token, unreachable source) carry no marker. They are reported as before: `401`/`403` as not authorized, `404` as not found, anything else as `500` with the message `Received code transferring HTTP data: <code> - <reason>.`.

## Limitations

- **Long-lived SSE streams.** A stateful MCP server may keep the `GET` SSE stream open. Each open stream holds a thread of the proxy executor on each hop: 5 threads on the consumer data API (`edc.dpf.dataplane.consumer.gateway.thread.pool`), 10 on the provider public API. A stream that stays idle longer than the EDC HTTP client read timeout (`edc.http.client.timeout.read`, 30 s by default) is cut. Request/response calls are not affected. This limitation is not covered by the tests.
- **Repeated headers.** When a request header is repeated on several lines, only its first value is relayed.

## Testing

- **Unit tests** (`headers` package, `DataFlowStartMessageSupplierTest`, `ConsumerDataPlaneAuthorizationServiceTest`):

  ```bash
  ./gradlew :extensions:data-plane:data-plane-public-api-v2:test
  ```

- **Controller tests**, tagged `@ApiTest`:

  ```bash
  ./gradlew :extensions:data-plane:data-plane-public-api-v2:test -DincludeTags=ApiTest
  ```

- **`McpThroughDataPlaneIntegrationTest`.** A data plane runtime in the test JVM, with the provider token check stubbed and a consumer EDR seeded in the store. The official MCP Java SDK client calls a stateless and a stateful MCP server through both hops. The test also checks that an asset without `proxyHeaders` behaves as before.

  ```bash
  ./gradlew :extensions:data-plane:data-plane-public-api-v2:test --tests "*McpThroughDataPlaneIntegrationTest"
  ```

- For the whole path, with contract negotiation and transfer, see the MCP section of [`system-tests/readme.md`](../../../system-tests/readme.md).
