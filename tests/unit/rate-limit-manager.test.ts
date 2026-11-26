import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const TEST_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-rate-limit-manager-"));
process.env.DATA_DIR = TEST_DATA_DIR;

const core = await import("../../src/lib/db/core.ts");
const providersDb = await import("../../src/lib/db/providers.ts");
const resilienceSettings = await import("../../src/lib/resilience/settings.ts");
const rateLimitManager = await import("../../open-sse/services/rateLimitManager.ts");
const accountFallback = await import("../../open-sse/services/accountFallback.ts");

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function flushBackgroundWork() {
  await wait(50);
  await new Promise((resolve) => setImmediate(resolve));
}

async function resetStorage() {
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true });
  fs.mkdirSync(TEST_DATA_DIR, { recursive: true });
}

test.beforeEach(async () => {
  await resetStorage();
});

test.afterEach(async () => {
  await rateLimitManager.__resetRateLimitManagerForTests();
  await flushBackgroundWork();
});

test.after(async () => {
  await rateLimitManager.__resetRateLimitManagerForTests();
  await flushBackgroundWork();
  core.resetDbInstance();
  fs.rmSync(TEST_DATA_DIR, { recursive: true, force: true });
});

test("rate limit manager bypasses disabled connections and exposes inactive status", async () => {
  const result = await rateLimitManager.withRateLimit("openai", "disabled-conn", null, async () => {
    return "bypassed";
  });

  assert.equal(result, "bypassed");
  assert.deepEqual(rateLimitManager.getRateLimitStatus("openai", "disabled-conn"), {
    enabled: false,
    active: false,
    queued: 0,
    running: 0,
  });
  assert.deepEqual(rateLimitManager.getAllRateLimitStatus(), {});
});

test("rate limit manager never dispatches a request cancelled in the queue", async () => {
  await rateLimitManager.applyRequestQueueSettings({
    ...resilienceSettings.DEFAULT_RESILIENCE_SETTINGS.requestQueue,
    concurrentRequests: 1,
    minTimeBetweenRequestsMs: 0,
    maxWaitMs: 5000,
  });
  const connection = "conn-cancel-queued";
  rateLimitManager.enableRateLimitProtection(connection);
  const started = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const schedule = (fn: () => Promise<unknown>, signal?: AbortSignal) =>
    rateLimitManager.withRateLimit("openai", connection, null, fn, signal);
  const active = schedule(async () => {
    started.resolve();
    await release.promise;
  });
  let dispatches = 0;
  try {
    await started.promise;
    const controller = new AbortController();
    const cancelled = schedule(async () => { dispatches += 1; }, controller.signal);
    const rejection = assert.rejects(cancelled, { name: "AbortError" });
    const deadline = performance.now() + 1000;
    while (rateLimitManager.getRateLimitStatus("openai", connection).queued === 0 &&
        performance.now() < deadline) {
      await wait(5);
    }
    const queued = rateLimitManager.getRateLimitStatus("openai", connection).queued;
    controller.abort(Object.assign(new Error("fixture cancelled"), { name: "AbortError" }));
    await rejection;
    assert.equal(queued, 1);
  } finally {
    release.resolve();
    await active;
  }
  assert.equal(await schedule(async () => "recovered"), "recovered");
  assert.equal(dispatches, 0);
});

test("the disabled limiter also honors a pre-aborted signal", async () => {
  await assert.rejects(rateLimitManager.withRateLimit(
    "openai", "disabled-conn", null, async () => assert.fail("unexpected dispatch"),
    AbortSignal.abort()
  ), { name: "AbortError" });
});

test("the limiter preserves the native abort reason while a request is scheduled", async () => {
  const connection = "conn-native-cancel";
  rateLimitManager.enableRateLimitProtection(connection);
  const controller = new AbortController();
  const started = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const request = rateLimitManager.withRateLimit("openai", connection, null, async () => {
    started.resolve();
    await release.promise;
  }, controller.signal);
  const rejected = assert.rejects(request, { name: "AbortError" });
  await started.promise;
  try {
    controller.abort();
    await rejected;
  } finally {
    release.resolve();
  }
});

test("rate limit manager handles soft over-limit warnings and normal header learning", async () => {
  rateLimitManager.enableRateLimitProtection("conn-over-limit");
  rateLimitManager.updateFromHeaders(
    "openai",
    "conn-over-limit",
    { "x-ratelimit-over-limit": "yes" },
    200
  );

  const softStatus = rateLimitManager.getRateLimitStatus("openai", "conn-over-limit");
  assert.equal(softStatus.enabled, true);
  assert.equal(softStatus.active, true);

  rateLimitManager.enableRateLimitProtection("conn-low-remaining");
  rateLimitManager.updateFromHeaders(
    "openai",
    "conn-low-remaining",
    {
      "x-ratelimit-limit-requests": "100",
      "x-ratelimit-remaining-requests": "5",
      "x-ratelimit-reset-requests": "30s",
    },
    200
  );
  await rateLimitManager.__flushLearnedLimitsForTests();

  const learnedLimits = rateLimitManager.getLearnedLimits();
  const learnedEntry = learnedLimits["openai:conn-low-remaining"];
  assert.equal(learnedEntry.provider, "openai");
  assert.equal(learnedEntry.connectionId, "conn-low-remaining");
  assert.equal(learnedEntry.limit, 100);
  assert.equal(learnedEntry.remaining, 5);
  assert.ok(learnedEntry.minTime > 0);

  rateLimitManager.enableRateLimitProtection("conn-high-remaining");
  rateLimitManager.updateFromHeaders(
    "claude",
    "conn-high-remaining",
    {
      get(name) {
        const map = {
          "anthropic-ratelimit-requests-limit": "100",
          "anthropic-ratelimit-requests-remaining": "70",
          "anthropic-ratelimit-requests-reset": new Date(Date.now() + 30_000).toISOString(),
        };
        return map[name] ?? null;
      },
    },
    200
  );
  await rateLimitManager.__flushLearnedLimitsForTests();

  const allStatuses = rateLimitManager.getAllRateLimitStatus();
  assert.ok(allStatuses["openai:conn-over-limit"]);
  assert.ok(allStatuses["openai:conn-low-remaining"]);
  assert.ok(allStatuses["claude:conn-high-remaining"]);
});

test("rate limit manager handles 429 limiter teardown and disable cleanup", async () => {
  rateLimitManager.enableRateLimitProtection("conn-429");
  rateLimitManager.updateFromHeaders("openai", "conn-429", { "retry-after": "1s" }, 429, "gpt-4o");
  await wait(25);

  assert.equal(rateLimitManager.getRateLimitStatus("openai", "conn-429").active, false);

  rateLimitManager.enableRateLimitProtection("conn-disable");
  rateLimitManager.updateFromHeaders(
    "gemini",
    "conn-disable",
    {
      "x-ratelimit-limit-requests": "60",
      "x-ratelimit-remaining-requests": "4",
      "x-ratelimit-reset-requests": "10s",
    },
    200,
    "gemini-2.5-flash"
  );
  await rateLimitManager.__flushLearnedLimitsForTests();
  assert.ok(rateLimitManager.getAllRateLimitStatus()["gemini:conn-disable:gemini-2.5-flash"]);

  rateLimitManager.disableRateLimitProtection("conn-disable");
  assert.equal(rateLimitManager.isRateLimitEnabled("conn-disable"), false);
  assert.equal(rateLimitManager.getRateLimitStatus("gemini", "conn-disable").active, false);
});

test("rate limit manager uses model-scoped limiter keys for GitHub Copilot (#1624)", async () => {
  rateLimitManager.enableRateLimitProtection("conn-github");
  rateLimitManager.updateFromHeaders(
    "github",
    "conn-github",
    {
      "x-ratelimit-limit-requests": "50",
      "x-ratelimit-remaining-requests": "3",
      "x-ratelimit-reset-requests": "15s",
    },
    200,
    "gpt-5.1-codex-max"
  );
  await rateLimitManager.__flushLearnedLimitsForTests();

  // GitHub should use model-scoped key: github:conn-github:gpt-5.1-codex-max
  const allStatuses = rateLimitManager.getAllRateLimitStatus();
  assert.ok(
    allStatuses["github:conn-github:gpt-5.1-codex-max"],
    "GitHub limiter key should be model-scoped (github:conn:model)"
  );
  // Verify the limiter state is model-scoped via test helper
  const limiterState = await rateLimitManager.__getLimiterStateForTests(
    "github",
    "conn-github",
    "gpt-5.1-codex-max"
  );
  assert.equal(limiterState?.key, "github:conn-github:gpt-5.1-codex-max");
});

test("rate limit manager parses retry hints from response bodies and locks models", async () => {
  rateLimitManager.enableRateLimitProtection("conn-body");
  rateLimitManager.updateFromResponseBody(
    "openai",
    "conn-body",
    {
      error: {
        details: [{ retryDelay: "2s" }],
        message: "Please retry later",
      },
    },
    429,
    "gpt-4o"
  );

  assert.equal(accountFallback.getModelLockoutInfo("openai", "conn-body", "gpt-4o"), null);
  const limiterState = await rateLimitManager.__getLimiterStateForTests(
    "openai",
    "conn-body",
    "gpt-4o"
  );
  assert.equal(limiterState?.key, "openai:conn-body");
  assert.equal(rateLimitManager.getRateLimitStatus("openai", "conn-body").active, true);

  rateLimitManager.updateFromResponseBody(
    "openai",
    "conn-body",
    JSON.stringify({ error: { type: "rate_limit_error" } }),
    429,
    null
  );
  assert.equal(rateLimitManager.getRateLimitStatus("openai", "conn-body").active, true);
});

test("RATE_LIMIT_AUTO_ENABLE env var overrides dashboard auto-enable setting", async () => {
  const conn = await providersDb.createProviderConnection({
    provider: "openai",
    authType: "apikey",
    name: "Env Override",
    apiKey: "sk-env",
    isActive: true,
  });

  // Dashboard says auto-enable on, but env says off → off wins
  const original = process.env.RATE_LIMIT_AUTO_ENABLE;
  process.env.RATE_LIMIT_AUTO_ENABLE = "false";
  try {
    await rateLimitManager.initializeRateLimits();
    assert.equal(rateLimitManager.isRateLimitEnabled(conn.id), false);
  } finally {
    if (original === undefined) delete process.env.RATE_LIMIT_AUTO_ENABLE;
    else process.env.RATE_LIMIT_AUTO_ENABLE = original;
  }

  // Reset and verify the opposite: env=true forces on even when dashboard would be off
  await rateLimitManager.__resetRateLimitManagerForTests();
  process.env.RATE_LIMIT_AUTO_ENABLE = "true";
  try {
    await rateLimitManager.applyRequestQueueSettings({
      ...resilienceSettings.DEFAULT_RESILIENCE_SETTINGS.requestQueue,
      autoEnableApiKeyProviders: false,
    });
    assert.equal(rateLimitManager.isRateLimitEnabled(conn.id), true);
  } finally {
    if (original === undefined) delete process.env.RATE_LIMIT_AUTO_ENABLE;
    else process.env.RATE_LIMIT_AUTO_ENABLE = original;
  }
});

test("rate limit manager recomputes auto-enabled API key connections when queue settings change", async () => {
  const autoConnection = await providersDb.createProviderConnection({
    provider: "openai",
    authType: "apikey",
    name: "Auto OpenAI",
    apiKey: "sk-auto",
    isActive: true,
  });
  const explicitConnection = await providersDb.createProviderConnection({
    provider: "openai",
    authType: "apikey",
    name: "Explicit OpenAI",
    apiKey: "sk-explicit",
    isActive: true,
    rateLimitProtection: true,
  });

  await rateLimitManager.initializeRateLimits();

  assert.equal(rateLimitManager.isRateLimitEnabled(autoConnection.id), true);
  assert.equal(rateLimitManager.isRateLimitEnabled(explicitConnection.id), true);
  assert.ok(rateLimitManager.getAllRateLimitStatus()[`openai:${autoConnection.id}`]);

  await rateLimitManager.applyRequestQueueSettings({
    ...resilienceSettings.DEFAULT_RESILIENCE_SETTINGS.requestQueue,
    autoEnableApiKeyProviders: false,
  });

  assert.equal(rateLimitManager.isRateLimitEnabled(autoConnection.id), false);
  assert.equal(rateLimitManager.isRateLimitEnabled(explicitConnection.id), true);
  assert.equal(rateLimitManager.getAllRateLimitStatus()[`openai:${autoConnection.id}`], undefined);

  await rateLimitManager.applyRequestQueueSettings({
    ...resilienceSettings.DEFAULT_RESILIENCE_SETTINGS.requestQueue,
    autoEnableApiKeyProviders: true,
  });

  assert.equal(rateLimitManager.isRateLimitEnabled(autoConnection.id), true);
  assert.equal(rateLimitManager.isRateLimitEnabled(explicitConnection.id), true);
  assert.ok(rateLimitManager.getAllRateLimitStatus()[`openai:${autoConnection.id}`]);
});

test("loads saved connection limits before starting an automatically enabled limiter", async () => {
  const connection = await providersDb.createProviderConnection({
    provider: "openai", authType: "apikey", name: "Saved OpenAI", apiKey: "fixture-key",
    isActive: true, rateLimitOverrides: { maxConcurrent: 32, minTime: 1, rpm: 37 },
  });
  await rateLimitManager.initializeRateLimits();
  assert.equal(rateLimitManager.isRateLimitEnabled(connection.id), true);
  const state = await rateLimitManager.__getLimiterStateForTests("openai", connection.id);
  assert.equal(state?.reservoir, 37);
});

test("refreshing global queue settings preserves the connection burst and the balance of its RPM bucket", async () => {
  const connection = await providersDb.createProviderConnection({
    provider: "openai", authType: "apikey", name: "Override OpenAI", apiKey: "fixture-key",
    isActive: true, rateLimitProtection: true, rateLimitOverrides: { maxConcurrent: 32, minTime: 1, rpm: 37 },
  });
  await rateLimitManager.initializeRateLimits();
  const schedule = (fn: () => Promise<void>) => rateLimitManager.withRateLimit("openai", connection.id, null, fn);
  await schedule(async () => {});
  await rateLimitManager.applyRequestQueueSettings({
    ...resilienceSettings.DEFAULT_RESILIENCE_SETTINGS.requestQueue,
    concurrentRequests: 2,
    minTimeBetweenRequestsMs: 250,
    requestsPerMinute: 90,
  });
  assert.equal((await rateLimitManager.__getLimiterStateForTests("openai", connection.id))?.reservoir, 36);

  const allActive = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let active = 0;
  let peak = 0;
  const jobs = Array.from({ length: 20 }, () => schedule(async () => {
    peak = Math.max(peak, ++active);
    if (peak === 20) allActive.resolve();
    await release.promise;
    active -= 1;
  }));
  try {
    const reached = await Promise.race([allActive.promise.then(() => true), wait(1000).then(() => false)]);
    assert.equal(reached, true);
    assert.equal(peak, 20);
    assert.equal((await rateLimitManager.__getLimiterStateForTests("openai", connection.id))?.reservoir, 16);
  } finally {
    release.resolve();
    await Promise.all(jobs);
  }
});
