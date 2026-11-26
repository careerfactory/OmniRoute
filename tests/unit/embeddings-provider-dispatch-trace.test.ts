import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import type { DispatchEvent } from "../../open-sse/utils/providerDispatchTrace.ts";

const dataDir = mkdtempSync(join(tmpdir(), "omniroute-voyage-dispatch-"));
process.env.DATA_DIR = dataDir;
process.env.API_KEY_SECRET ||= "embedding-route-fixture-secret-only";

test("Voyage embeddings correlate 20 simultaneous upstream dispatches and respect noLog", async (t) => {
  const { resetDbInstance } = await import("../../src/lib/db/core.ts");
  t.after(() => {
    resetDbInstance();
    rmSync(dataDir, { recursive: true, force: true });
  });
  const { POST } = await import("../../src/app/api/v1/embeddings/route.ts");
  const { createApiKey, updateApiKeyPermissions } = await import("../../src/lib/db/apiKeys.ts");
  const { createProviderConnection } = await import("../../src/lib/db/providers.ts");
  const key = await createApiKey("Embedding trace", "embedding-dispatch-fixture");
  await createProviderConnection({
    provider: "voyage-ai", authType: "apikey", name: "Voyage fixture", apiKey: "local-provider-key",
  });
  const priorKeyId = process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID;
  process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = key.id;
  const events: DispatchEvent[] = [];
  t.mock.method(console, "info", (message: unknown) => {
    const prefix = "[PROVIDER_DISPATCH] ";
    if (typeof message === "string" && message.startsWith(prefix)) {
      events.push(JSON.parse(message.slice(prefix.length)));
    }
  });

  const requests: { input: string; arrival: number; completed?: number }[] = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    assert.equal(request.headers.authorization, "Bearer local-provider-key");
    assert.equal(body.model, "voyage-3-large");
    const observed = { input: body.input, arrival: performance.now(), completed: undefined as number | undefined };
    requests.push(observed);
    await delay(2400);
    observed.completed = performance.now();
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      object: "list", data: [{ object: "embedding", index: 0, embedding: [.125, .25] }],
      usage: { prompt_tokens: 7, total_tokens: 7 },
    }));
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address === "object");
  const originalFetch = globalThis.fetch;
  t.mock.method(globalThis, "fetch", (url: string, options: RequestInit) => {
    assert.equal(url, "https://api.voyageai.com/v1/embeddings");
    return originalFetch(`http://127.0.0.1:${address.port}/v1/embeddings`, options);
  });

  const invoke = async (input: string) => {
    const response = await POST(new Request("http://localhost/v1/embeddings", {
      method: "POST", headers: { Authorization: `Bearer ${key.key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ model: "voyage/voyage-3-large", input }),
    }));
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("x-omniroute-tokens-in"), "7");
    assert.deepEqual((await response.json()).data[0].embedding, [.125, .25]);
    const correlationId = response.headers.get("x-correlation-id");
    assert.ok(correlationId);
    return correlationId;
  };

  try {
    const started = performance.now();
    const inputs = Array.from({ length: 20 }, (_, index) => `unique input ${index}`);
    const correlations = await Promise.all(inputs.map(invoke));
    assert.equal(new Set(correlations).size, 20);
    assert.deepEqual(new Set(requests.map(request => request.input)), new Set(inputs));
    const last = Math.max(...requests.map(request => request.arrival));
    assert.ok(last - started < 1000, `last send after ${last - started} ms`);
    assert.ok(requests.every(request => last < (request.completed ?? 0)));
    for (const id of correlations) {
      const trace = events.filter(event => event.correlationId === id);
      assert.deepEqual(trace.map(event => event.phase), ["created", "body_sent", "headers", "completed"]);
      assert.ok(trace.every(event => event.attempt === 1));
    }
    assert.equal(events.length, 80);
    const cancelled = await POST(new Request("http://localhost/v1/embeddings", {
      method: "POST", headers: { Authorization: `Bearer ${key.key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ model: "voyage/voyage-3-large", input: "cancelled request" }),
      signal: AbortSignal.abort(),
    }));
    assert.equal(cancelled.status, 499);
    assert.equal(events.length, 80);
    assert.equal(requests.length, 20);
    await updateApiKeyPermissions(key.id, { noLog: true });
    const privateRequest = await invoke("noLog trace check");
    assert.ok(!events.some(event => event.correlationId === privateRequest));
    assert.equal(events.length, 80);
    assert.equal(requests.length, 21);
    console.log(JSON.stringify({ valid: 20, attempts: 20, peak: 20, lastDispatchMs: Math.round(last - started), noLogVerified: true, cancelledBeforeSend: true }));
  } finally {
    if (priorKeyId === undefined) delete process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID;
    else process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = priorKeyId;
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
});
