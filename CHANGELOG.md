# Changelog

## 1.0.1

### Fixed

- The node icon was drawn in white for the light theme and near-black for the dark theme, so in both
  cases the largest shape was invisible against the background and the icon appeared tiny. It is now
  n8n's own SSE Trigger mark encircled by a reconnect loop, with a colour per theme.

## 1.0.0

Initial release of the **Resilient SSE Trigger** node, which starts an n8n workflow from a
Server-Sent Events stream and is built to stay connected.

### Streaming

- Emits one workflow item per received event.
- Parses JSON payloads into the item's JSON, and passes a non-JSON payload through as
  `{ "data": "<raw text>" }` so an unexpected frame never ends the stream.
- Optionally attaches a `$metadata` key carrying the event type, last event ID, origin URL, and
  receive timestamp.

### Reconnecting

- Reconnects after a retryable HTTP error, a mid-stream drop, a connection timeout, or the server
  closing the stream cleanly.
- Exponential backoff with a configurable initial delay, maximum delay, backoff factor, attempt
  limit, and optional jitter.
- Resumes with the `Last-Event-ID` header so a server that supports replay can pick up where the
  workflow left off.
- Optionally honours the delay the stream requests through its `retry:` field.
- Stops and reports a fatal error when retrying cannot help, rather than looping: a 4xx response
  other than 408 or 429, or a credential that will not resolve.

### Authentication

- Bearer token and custom header credentials, stored encrypted by n8n.
- Additional request headers as name/value pairs, as a JSON object, or both.

### Visibility

- Writes every connection state change to the n8n logs.
- Optionally emits each state change as a workflow item for inspection in the editor.

### Packaging

- No runtime dependencies; the SSE framing is parsed over the runtime's global `fetch`.
