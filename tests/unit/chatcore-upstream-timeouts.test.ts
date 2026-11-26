import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";

import {
  createBodyTimeoutError,
  createUpstreamStartTimeoutError,
  createAbortError,
  computeBillableTokens,
  getExecutorTimeoutMs,
  normalizeExecutorResult,
  executeWithUpstreamStartTimeout,
} from "../../open-sse/handlers/chatCore/upstreamTimeouts.ts";

test("error factories set name and message", () => {
  const body = createBodyTimeoutError(1234);
  assert.equal(body.name, "BodyTimeoutError");
  assert.match(body.message, /1234ms/);

  const start = createUpstreamStartTimeoutError(500, "openai", "gpt-4o");
  assert.equal(start.name, "TimeoutError");
  assert.match(start.message, /openai\/gpt-4o/);

  const ctrl = new AbortController();
  ctrl.abort("nope");
  const ab = createAbortError(ctrl.signal);
  assert.equal(ab.name, "AbortError");
});

test("computeBillableTokens sums input+output+reasoning (no cache double-count)", () => {
  const total = computeBillableTokens({
    prompt_tokens: 10,
    completion_tokens: 5,
    reasoning_tokens: 2,
  });
  assert.equal(total, 17);
});

test("getExecutorTimeoutMs floors valid values and falls back to default", () => {
  assert.equal(getExecutorTimeoutMs({ getTimeoutMs: () => 1234.9 }), 1234);
  assert.equal(getExecutorTimeoutMs({ getTimeoutMs: () => NaN }), getExecutorTimeoutMs(null));
  assert.ok(Number.isFinite(getExecutorTimeoutMs(null)));
});

test("normalizeExecutorResult wraps bare Response and passes through rich result", () => {
  const r = new Response("x");
  const wrapped = normalizeExecutorResult(r);
  assert.equal(wrapped.response, r);
  assert.equal(wrapped.url, "");
  const rich = normalizeExecutorResult({ response: r, url: "u", headers: { a: "b" } });
  assert.equal(rich.url, "u");
  assert.equal(rich.headers.a, "b");
});

test("client abort after upstream headers closes the active SSE connection", { timeout: 5000 }, async () => {
  const closed = Promise.withResolvers<void>();
  const server = createServer((_req, response) => {
    response.once("close", closed.resolve);
    response.writeHead(200, { "Content-Type": "text/event-stream" });
    response.write("data: first\n\n");
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const client = new AbortController();
    const upstream = await executeWithUpstreamStartTimeout({
      executor: { getTimeoutMs: () => 2000 },
      provider: "openrouter",
      model: "deepseek",
      signal: client.signal,
      execute: (signal) => fetch(`http://127.0.0.1:${address.port}/`, { signal }),
    });
    const reader = upstream.body?.getReader();
    assert.ok(reader);
    assert.match(new TextDecoder().decode((await reader.read()).value), /data: first/);
    client.abort();
    await Promise.race([
      closed.promise,
      delay(500).then(() => assert.fail("upstream SSE connection stayed open after client abort")),
    ]);
    await assert.rejects(reader.read(), { name: "AbortError" });
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test("the timeout for upstream headers does not expire a live response", async () => {
  const client = new AbortController();
  const upstreamSignal = await executeWithUpstreamStartTimeout({
    executor: { getTimeoutMs: () => 20 },
    provider: "openrouter",
    model: "deepseek",
    signal: client.signal,
    execute: async (signal) => signal,
  });
  await delay(50);
  assert.equal(upstreamSignal.aborted, false);
  client.abort("client closed");
  assert.equal(upstreamSignal.aborted, true);
});
