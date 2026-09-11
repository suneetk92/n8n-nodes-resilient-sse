# n8n-nodes-resilient-sse

This is an n8n community node. It lets you start workflows from a [Server-Sent Events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events) (SSE) stream.

Its focus is staying connected. n8n's built-in SSE Trigger reconnects on a fixed 3-second interval and gives up permanently when the server returns an HTTP error. This node reconnects with exponential backoff and jitter, resumes with `Last-Event-ID`, honours the stream's own `retry:` hint, and surfaces the connection state so you can see what it is doing.

[n8n](https://n8n.io/) is a [fair-code licensed](https://docs.n8n.io/sustainable-use-license/) workflow automation platform.

[Installation](#installation)
[Operations](#operations)
[Credentials](#credentials)
[Compatibility](#compatibility)
[Usage](#usage)
[Resources](#resources)
[Version history](#version-history)

## Installation

Follow the [installation guide](https://docs.n8n.io/integrations/community-nodes/installation/) in the n8n community nodes documentation.

To install an unpublished local build instead, build a tarball with `npm pack` and install it into your n8n nodes directory:

```sh
mkdir -p ~/.n8n/nodes && cd ~/.n8n/nodes
npm install --omit=peer /path/to/n8n-nodes-resilient-sse-1.0.0.tgz
```

Then restart n8n. `--omit=peer` matters: without it npm tries to install the `n8n-workflow` peer dependency, which pulls in `isolated-vm` and fails compiling native code. n8n provides `n8n-workflow` itself.

## Operations

The node connects to an SSE endpoint and emits one workflow item per received event.

Event payloads are parsed as JSON and emitted as the item's JSON. A payload that is not valid JSON — a plain-text heartbeat, a `[DONE]` sentinel — is emitted as `{ "data": "<raw text>" }` instead, so an unexpected frame never tears the stream down. Arrays and scalars are wrapped the same way, since spreading them would produce numeric keys.

With **Include Metadata** on (the default), each item also carries a `$metadata` key:

```json
{
  "seq": 9,
  "$metadata": {
    "eventType": "update",
    "lastEventId": "evt-9",
    "origin": "http://example.com/events",
    "timestamp": "2026-09-10T12:00:00.000Z"
  }
}
```

`eventType` is the stream's `event:` field, or `message` when it is absent — this is how you tell named events apart.

## Credentials

Two optional credential types, selected by the node's **Authentication** parameter:

| Credential | Sends |
| --- | --- |
| **SSE Bearer Auth API** | `Authorization: Bearer <token>` |
| **SSE Header Auth API** | A header of your choosing, e.g. `X-API-Key: <value>` |

Both store their secret encrypted at rest, which is why they are preferable to putting a token into a custom header in the node parameters.

Neither carries a service URL of its own — the endpoint lives on the node — so there is nothing to send a probe request to. The **Test** button therefore only confirms the fields are filled in; the credential is genuinely proven the first time the trigger connects.

Choose **None** for unauthenticated endpoints.

## Compatibility

Requires an n8n version whose runtime provides the global `fetch` API (Node.js 18+). The node has no runtime npm dependencies.

## Usage

### Parameters

Three parameters are always visible:

| Parameter | Default | Description |
| --- | --- | --- |
| **URL** | – | The SSE endpoint to connect to. Required. |
| **Authentication** | `None` | `None`, `Bearer Auth`, or `Header Auth`. See [Credentials](#credentials). |
| **Auto Reconnect** | `true` | Reconnect when the stream ends, the connection drops, or the server returns a retryable error. |

Everything else lives under **Additional Fields**, and the defaults below apply whether or not you add the field:

| Field | Default | Description |
| --- | --- | --- |
| **Add Jitter** | `true` | Randomises each retry delay between 50% and 100% of its computed value. |
| **Backoff Factor** | `2` | Multiplier applied per consecutive failure. Use `1` for a constant delay. |
| **Connection Timeout (Ms)** | `30000` | How long to wait for the server's response headers. `0` waits indefinitely. Does not limit how long an open stream stays open. |
| **Emit Connection Status** | `false` | Also emit an item on every connection state change. |
| **Headers** | – | Extra headers as name/value pairs, sent alongside any authentication. |
| **Headers (JSON)** | `{}` | Extra headers as a JSON object, for when the whole set comes from one expression. Merged with **Headers**, which wins on conflict. |
| **Honor Server Retry Hint** | `true` | Use the delay from the stream's `retry:` field instead of the computed one. Still capped by Max Retry Delay. |
| **Include Metadata** | `true` | Add the `$metadata` key described above to each item. |
| **Initial Retry Delay (Ms)** | `1000` | Delay before the first reconnect attempt. |
| **Max Retry Attempts** | `0` | Consecutive attempts before giving up. `0` retries forever. Resets on a successful connection. |
| **Max Retry Delay (Ms)** | `30000` | Upper bound on the delay after the backoff factor is applied. |

Retry delays are floored at 50 ms, so a zero or malformed delay cannot turn reconnecting into a hot loop.

### How reconnecting works

A reconnect is triggered by a retryable HTTP error, a mid-stream connection drop, a connection timeout, or the server closing the stream cleanly. The delay is `Initial Retry Delay × Backoff Factor ^ (attempt - 1)`, capped at Max Retry Delay, then optionally jittered.

The node tracks the stream's `id:` field and sends it back as the `Last-Event-ID` header on every reconnect, so a server that supports replay can resume where the workflow left off.

**Client errors are not retried.** A 4xx response other than `408 Request Timeout` or `429 Too Many Requests` means the request itself was refused, so repeating it unchanged cannot succeed. The node stops and reports a fatal error rather than hammering the endpoint — check the URL, the credential, and any custom headers.

When Max Retry Attempts is exceeded, the node likewise reports a fatal error to n8n, which deactivates the workflow and retries activation with its own backoff.

### Seeing the connection status

Every state change (`connecting`, `connected`, `disconnected`, `error`, `reconnecting`, `stopped`) is written to the n8n logs.

Enable **Emit Connection Status** to receive the same transitions as workflow items, which is the easiest way to see them in the editor:

```json
{
  "status": "reconnecting",
  "url": "http://example.com/events",
  "attempt": 2,
  "message": "Stream closed by the server",
  "timestamp": "2026-09-10T12:00:00.000Z"
}
```

Status items are emitted on the same output as data items, so branch on the `status` key with an `If` or `Switch` node if you need to separate them.

Be aware of the cost: n8n starts a **separate workflow execution per emitted item**, so a single connection flap produces three executions (`error`, `reconnecting`, `connected`). Against a flapping endpoint this fills your execution history quickly. Leave it off in production and turn it on when diagnosing a connection problem.

## Resources

* [n8n community nodes documentation](https://docs.n8n.io/integrations/#community-nodes)
* [SSE specification (WHATWG)](https://html.spec.whatwg.org/multipage/server-sent-events.html)

## Version history

### 1.0.0

Initial release.
