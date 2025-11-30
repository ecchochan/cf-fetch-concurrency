# Cloudflare Workers Concurrency Test

A Cloudflare Worker that tests whether concurrent outgoing requests are throttled by the ["Simultaneous open connections" limit](https://developers.cloudflare.com/workers/platform/limits/#simultaneous-open-connections).

## Test Design

The worker fires N concurrent requests to Durable Objects and measures `queueDelay` — the time between request initiation and when the target DO begins processing. If connection queueing were enforced, requests exceeding the limit would show significantly higher delays.

### Endpoints

| Path | Description |
|------|-------------|
| `/test/do/rpc` | DO-to-DO via RPC |
| `/test/do/fetch` | DO-to-DO via fetch |
| `/test/worker/rpc` | Worker-to-DO via RPC |
| `/test/worker/fetch` | Worker-to-DO via fetch |

**Parameters:** `calls=N`, `delay=MS`, `same=true|false` (target same or different DOs)

## Results

Queue delays are consistently under 1 second across all test modes, suggesting the measured delay is purely network latency with **no observable connection queueing**.

## Usage

```bash
pnpm install
pnpm dev          # local development
pnpm deploy       # deploy to Cloudflare

# Example test: 50 concurrent calls, 100ms delay each, same target DO
curl "http://localhost:8787/test/do/rpc?calls=50&delay=100&same=true"
```
