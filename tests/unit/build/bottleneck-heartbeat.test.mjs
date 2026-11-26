import assert from "node:assert/strict";
import test from "node:test";
import Bottleneck from "bottleneck";

test("updates preserve recurring reservoir refresh and allow the timer to restart", { timeout: 4000 }, async () => {
  const limiter = new Bottleneck({
    reservoir: 1,
    reservoirRefreshAmount: 1,
    reservoirRefreshInterval: 250,
  });
  try {
    await limiter.schedule(() => "initial");
    await limiter.updateSettings({ minTime: 1 });
    for (const value of ["first refill", "second refill"]) {
      assert.equal(await limiter.schedule(() => value), value);
      await limiter.updateSettings({ minTime: 1 });
    }

    await limiter.updateSettings({ reservoirRefreshInterval: null });
    await limiter.updateSettings({ reservoirRefreshInterval: 250 });
    assert.equal(await limiter.schedule(() => "restarted"), "restarted");
  } finally {
    await limiter.stop({ dropWaitingJobs: true });
    await limiter.disconnect();
  }
});
