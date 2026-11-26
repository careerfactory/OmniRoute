import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { createProviderDispatchTrace, type DispatchEvent } from "../../open-sse/utils/providerDispatchTrace.ts";

import {
  runWithCapture,
  type Capture,
  type ProviderRequestPrepared,
} from "../../open-sse/utils/providerRequestLogging.ts";

test("runWithCapture captures the actual JSON provider fetch body", async () => {
  const originalFetch = globalThis.fetch;
  const prepared: ProviderRequestPrepared[] = [];
  const sentBodies: unknown[] = [];
  const capture: Capture = {
    capture(request) {
      prepared.push(request);
    },
    body(fallback) {
      return prepared.at(-1)?.body ?? fallback;
    },
  };

  globalThis.fetch = async (_url, init = {}) => {
    sentBodies.push(JSON.parse(String(init.body)));
    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    await runWithCapture(capture, () =>
      fetch("https://provider.example/v1/chat/completions", {
        method: "POST",
        headers: { Authorization: "Bearer provider-key" },
        body: JSON.stringify({
          model: "claude-sonnet-4-6",
          messages: [{ role: "user", content: "hi" }],
          reasoning_effort: "high",
        }),
      })
    );

    assert.equal(prepared.length, 1);
    assert.deepEqual(prepared[0].body, sentBodies[0]);
    assert.equal(prepared[0].url, "https://provider.example/v1/chat/completions");
    assert.equal(prepared[0].headers.authorization, "Bearer provider-key");
    assert.equal((capture.body(null) as Record<string, unknown>).reasoning_effort, "high");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("runWithCapture ignores auth fetch bodies in the same executor scope", async () => {
  const originalFetch = globalThis.fetch;
  const prepared: ProviderRequestPrepared[] = [];
  const capture: Capture = {
    capture(request) {
      prepared.push(request);
    },
    body(fallback) {
      return prepared.at(-1)?.body ?? fallback;
    },
  };

  globalThis.fetch = async () =>
    new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });

  try {
    await runWithCapture(capture, async () => {
      await fetch("https://oauth.example/token", {
        method: "POST",
        body: JSON.stringify({
          grant_type: "refresh_token",
          refresh_token: "refresh",
          client_id: "client",
        }),
      });
      await fetch("https://provider.example/v1/chat/completions", {
        method: "POST",
        body: JSON.stringify({
          model: "gpt-5",
          messages: [{ role: "user", content: "hi" }],
        }),
      });
    });

    assert.equal(prepared.length, 1);
    assert.equal((prepared[0].body as Record<string, unknown>).model, "gpt-5");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("runWithCapture does not duplicate an already prepared identical fetch", async () => {
  const originalFetch = globalThis.fetch;
  const prepared: ProviderRequestPrepared[] = [];
  const body = {
    model: "gpt-5",
    messages: [{ role: "user", content: "hi" }],
  };
  const bodyString = JSON.stringify(body);
  const url = "https://provider.example/v1/chat/completions";
  let latest: ProviderRequestPrepared | null = null;
  const capture: Capture = {
    capture(request) {
      latest = request;
      prepared.push(request);
    },
    body(fallback) {
      return latest?.body ?? fallback;
    },
    latest() {
      return latest;
    },
  };

  globalThis.fetch = async () =>
    new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });

  try {
    await runWithCapture(capture, async () => {
      await capture.capture({ url, headers: {}, body, bodyString });
      await fetch(url, {
        method: "POST",
        body: bodyString,
      });
    });

    assert.equal(prepared.length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("the scoped transport trace counts 20 actual sends and keeps them open until response completion", async () => {
  let arrived = 0;
  const replies: (() => void)[] = [];
  const server = createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      arrived++;
      if (arrived > 20) {
        res.end("{}\n");
        return;
      }
      replies.push(() => res.end("{}\n"));
      if (arrived === 20) replies.forEach((reply) => reply());
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const url = `http://127.0.0.1:${address.port}/inference`;
  const originalFetch = globalThis.fetch;
  const originalKey = process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID;
  process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = "fixture-key";
  const events: DispatchEvent[] = [];
  const trace = (id: string, key = "fixture-key") => createProviderDispatchTrace(key, id, (event) => events.push(event));
  const send = (dispatchTrace: Capture["dispatchTrace"], signal = AbortSignal.timeout(5000)) =>
    runWithCapture({ dispatchTrace, capture() {}, body: (fallback) => fallback }, async () => {
      const response = await fetch(url, { method: "POST", body: '{"model":"fixture"}', signal });
      await response.text();
    });
  try {
    await Promise.all(Array.from({ length: 20 }, (_, i) => send(trace(`request-${i}`))));
    const sent = events.filter((e) => e.phase === "body_sent");
    const completed = events.filter((e) => e.phase === "completed");
    assert.equal(new Set(sent.map((e) => e.correlationId)).size, 20);
    assert.equal(completed.length, 20);
    assert.ok(Math.max(...sent.map((e) => e.monotonicMs)) < Math.min(...completed.map((e) => e.monotonicMs)));
    const repeat = trace("repeated");
    await send(repeat);
    await send(repeat);
    await send(trace("unscoped", "other"));
    await assert.rejects(send(trace("cancelled"), AbortSignal.abort()));
    assert.deepEqual(events.filter((e) => e.correlationId === "repeated" && e.phase === "body_sent").map((e) => e.attempt), [1, 2]);
    assert.equal(events.some((e) => e.correlationId === "unscoped" || e.correlationId === "cancelled"), false);
    assert.equal(arrived, 23);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalKey === undefined) delete process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID;
    else process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = originalKey;
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

test("diagnostic dispatch tracing supports bounded explicit key selection", () => {
  const original = process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID;
  const keys = ["fixture-a", "fixture-b", "fixture-c", "fixture-d", "fixture-e"];
  const trace = (key: string) => createProviderDispatchTrace(key, "fixture-correlation");
  try {
    process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = keys.join(", ");
    for (const key of keys) assert.ok(trace(key));
    assert.equal(trace("fixture-other"), undefined);

    process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = keys[0];
    assert.ok(trace(keys[0]));
    assert.equal(trace(keys[1]), undefined);

    process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = [...keys, "fixture-f"].join(",");
    assert.equal(trace(keys[0]), undefined);
    process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = "fixture-a,,fixture-b";
    assert.equal(trace(keys[0]), undefined);
  } finally {
    if (original === undefined) delete process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID;
    else process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = original;
  }
});
