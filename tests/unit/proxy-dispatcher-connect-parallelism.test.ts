import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { createSecureServer } from "node:http2";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import tls from "node:tls";
import { fetch } from "undici";
import { clearDispatcherCache, createProxyDispatcher, requiresCancellableSseTransport } from "../../open-sse/utils/proxyDispatcher.ts";
import {
  createProviderDispatchTrace,
  runWithProviderDispatchTrace,
  type DispatchEvent,
} from "../../open-sse/utils/providerDispatchTrace.ts";

function listen(server: net.Server): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      assert.ok(address && typeof address !== "string");
      resolve(address.port);
    });
  });
}

test("twenty staggered POSTs through HTTP CONNECT reach the HTTP/2 upstream concurrently", {
  timeout: 20000,
}, async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "omni-proxy-tls-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const keyPath = join(directory, "key.pem");
  const certPath = join(directory, "cert.pem");
  execFileSync("openssl", [
    "req", "-new", "-newkey", "rsa:2048", "-x509", "-nodes", "-days", "1",
    "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1",
    "-keyout", keyPath, "-out", certPath,
  ], { stdio: "ignore" });
  const cert = readFileSync(certPath);
  const priorCas = tls.getCACertificates("default");
  tls.setDefaultCACertificates([...priorCas, cert.toString()]);
  t.after(() => tls.setDefaultCACertificates(priorCas));
  const previousKey = process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID;
  process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = "fixture";
  t.after(() => {
    if (previousKey === undefined) delete process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID;
    else process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID = previousKey;
  });

  const requests = new Map<string, { arrival: number; completed?: number }>();
  let http2Count = 0;
  const upstream = createSecureServer({
    allowHTTP1: true, settings: { maxConcurrentStreams: 5 }, key: readFileSync(keyPath), cert,
  }, async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk;
    if (request.httpVersionMajor === 2) http2Count++;
    const entry: { arrival: number; completed?: number } = { arrival: performance.now() };
    requests.set(body, entry);
    await delay(2400);
    entry.completed = performance.now();
    response.end("ok");
  });
  const upstreamPort = await listen(upstream);
  t.after(() => { upstream.close(); });

  let tunnels = 0;
  const proxy = createServer();
  proxy.on("connect", (request, client, head) => {
    if (request.url !== `127.0.0.1:${upstreamPort}`) {
      client.destroy();
      return;
    }
    tunnels++;
    const target = net.connect(upstreamPort, "127.0.0.1", () => {
      client.write("HTTP/1.1 200 Connection Established\r\n\r\n");
      if (head.length) target.write(head);
      client.pipe(target);
      target.pipe(client);
    });
    target.on("error", () => client.destroy());
    client.on("error", () => target.destroy());
    client.on("close", () => target.destroy());
  });
  const proxyPort = await listen(proxy);
  t.after(() => { proxy.close(); });

  const dispatcher = createProxyDispatcher(`http://127.0.0.1:${proxyPort}`);
  t.after(async () => { await dispatcher.destroy(); clearDispatcherCache(); });
  const events: DispatchEvent[] = [];
  const started = performance.now();
  await Promise.all(Array.from({ length: 20 }, async (_, index) => {
    await delay(index * 18);
    const reference = `request-${index}`;
    const trace = createProviderDispatchTrace("fixture", reference, (event) => events.push(event));
    assert.ok(trace);
    const response = await runWithProviderDispatchTrace(trace, () => fetch(
      `https://127.0.0.1:${upstreamPort}/v1/chat/completions`,
      { dispatcher, method: "POST", body: reference, signal: AbortSignal.timeout(17000) }
    ));
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "ok");
  }));

  const arrivals = [...requests.values()];
  const last = Math.max(...arrivals.map(row => row.arrival));
  assert.equal(requests.size, 20);
  assert.equal(http2Count, 20);
  assert.equal(tunnels, 20);
  assert.ok(last - started < 1000, `last arrival after ${last - started} ms`);
  assert.ok(arrivals.every(row => (row.completed ?? 0) > last));
  for (const reference of requests.keys()) {
    const trace = events.filter(event => event.correlationId === reference);
    assert.deepEqual(trace.map(event => event.phase), ["created", "body_sent", "headers", "completed"]);
    assert.ok(trace.every(event => event.attempt === 1));
  }
  assert.equal(events.length, 80);
  console.log(JSON.stringify({ sent: 20, valid: 20, peak: 20, lastArrivalMs: Math.round(last - started), tunnels }));
});

test("the cancellable transport is selected only for streaming requests to a configured origin", async () => {
  const prior = process.env.OMNIROUTE_CANCELLABLE_SSE_ORIGINS;
  process.env.OMNIROUTE_CANCELLABLE_SSE_ORIGINS = "https://a.example, https://b.example";
  try {
    const signal = new AbortController().signal;
    const body = JSON.stringify({ stream: true });
    assert.equal(requiresCancellableSseTransport("https://b.example/v1/chat/completions", { body, signal }), true);
    assert.equal(requiresCancellableSseTransport("https://b.example:8443/v1/chat/completions", { body, signal }), false);
    assert.equal(requiresCancellableSseTransport("https://other.example/v1/chat/completions", { body, signal }), false);
    assert.equal(requiresCancellableSseTransport("https://b.example/v1/chat/completions", { body }), false);
    assert.equal(requiresCancellableSseTransport("https://b.example/v1/chat/completions", { body: '{"stream":false}', signal }), false);
  } finally {
    if (prior === undefined) delete process.env.OMNIROUTE_CANCELLABLE_SSE_ORIGINS;
    else process.env.OMNIROUTE_CANCELLABLE_SSE_ORIGINS = prior;
  }
});
