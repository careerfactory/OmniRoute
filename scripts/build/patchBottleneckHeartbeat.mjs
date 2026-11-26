import { readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

// Bottleneck 2.19.5 clears a running heartbeat on updateSettings(), even
// when the updated settings still require both the timer and the reservoir.
export function patchBottleneckHeartbeat(
  file = require.resolve("bottleneck/lib/LocalDatastore.js")
) {
  const source = readFileSync(file, "utf8");
  const start = "if (this.heartbeat == null && (this.storeOptions.reservoirRefreshInterval";
  const end = "this.storeOptions.reservoirIncreaseAmount != null)) {";
  const clear = "    } else {\n      return clearInterval(this.heartbeat);\n    }";
  const patchedStart = "if (this.storeOptions.reservoirRefreshInterval";
  const patchedEnd = end.replace(")) {", ") {") + "\n      if (this.heartbeat != null) return;";
  const patchedClear =
    "    } else if (this.heartbeat != null) {\n" +
    "      clearInterval(this.heartbeat);\n" +
    "      this.heartbeat = null;\n    }";
  if (source.includes(patchedEnd) && source.includes(patchedClear)) return;
  if (![start, end, clear].every(fragment => source.split(fragment).length === 2)) {
    throw new Error("Unexpected Bottleneck heartbeat implementation. Verify the pinned package patch.");
  }
  writeFileSync(file, source.replace(start, patchedStart).replace(end, patchedEnd).replace(clear, patchedClear));
}
