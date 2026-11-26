import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import { proxyRequest } from "../../src/lib/apiBridgeServer.ts";

async function listen(server: Server) {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address === "object");
  return address.port;
}

test("closing an SSE response after a complete upload closes the dashboard connection", { timeout: 5000 }, async () => {
  const dashboardConnectionClosed = Promise.withResolvers<void>();
  const dashboard = createServer(async (req, res) => {
    for await (const _chunk of req) {
      // Consume the upload before sending the SSE response.
    }
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.once("close", dashboardConnectionClosed.resolve);
    res.write("data: first\n\n");
  });
  const dashboardPort = await listen(dashboard);
  const bridge = createServer((req, res) => proxyRequest(req, res, dashboardPort));
  const apiPort = await listen(bridge);
  const client = new AbortController();
  try {
    const response = await fetch(`http://127.0.0.1:${apiPort}/v1/chat/completions`, {
      method: "POST",
      body: '{"stream":true}',
      signal: client.signal,
    });
    const reader = response.body?.getReader();
    assert.ok(reader);
    assert.match(new TextDecoder().decode((await reader.read()).value), /data: first/);
    client.abort();
    await Promise.race([
      dashboardConnectionClosed.promise,
      delay(600).then(() => assert.fail("dashboard SSE connection stayed open after bridge client closed")),
    ]);
  } finally {
    client.abort();
    bridge.closeAllConnections();
    dashboard.closeAllConnections();
    bridge.close();
    dashboard.close();
  }
});
