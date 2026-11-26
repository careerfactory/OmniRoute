import { AsyncLocalStorage } from "node:async_hooks";
import { subscribe } from "node:diagnostics_channel";
import { performance } from "node:perf_hooks";

type Phase = "queue_enter" | "queue_exit" | "created" | "body_sent" | "headers" | "completed" | "error";
export type DispatchEvent = {
  correlationId: string;
  attempt: number | null;
  phase: Phase;
  monotonicMs: number;
  timestampMs: number;
  status?: number;
};

export type ProviderDispatchTrace = {
  phase: (phase: "queue_enter" | "queue_exit") => void;
  nextAttempt: () => number;
  emit: (phase: Phase, attempt: number | null, status?: number) => void;
};

type DiagnosticMessage = { request: object; response?: { statusCode?: number } };
const tracing = new AsyncLocalStorage<ProviderDispatchTrace>();
const attempts = new WeakMap<object, { trace: ProviderDispatchTrace; number: number }>();
let subscribed = false;

function traceAttempt(message: unknown, phase: Phase, terminal = false) {
  const { request, response } = message as DiagnosticMessage;
  const attempt = attempts.get(request);
  if (!attempt) return;
  if (terminal) attempts.delete(request);
  attempt.trace.emit(phase, attempt.number, response?.statusCode);
}

function subscribeToUndici() {
  if (subscribed) return;
  subscribed = true;
  subscribe("undici:request:create", (message: unknown) => {
    const trace = tracing.getStore();
    if (!trace) return;
    const { request } = message as DiagnosticMessage;
    const number = trace.nextAttempt();
    attempts.set(request, { trace, number });
    trace.emit("created", number);
  });
  subscribe("undici:request:bodySent", (message) => traceAttempt(message, "body_sent"));
  subscribe("undici:request:headers", (message) => traceAttempt(message, "headers"));
  subscribe("undici:request:trailers", (message) => traceAttempt(message, "completed", true));
  subscribe("undici:request:error", (message) => traceAttempt(message, "error", true));
}

export function createProviderDispatchTrace(
  keyId: string | undefined,
  correlationId: string | null | undefined,
  write: (event: DispatchEvent) => void = (event) =>
    console.info(`[PROVIDER_DISPATCH] ${JSON.stringify(event)}`)
): ProviderDispatchTrace | undefined {
  if (!keyId || keyId !== process.env.OMNIROUTE_PROVIDER_TRACE_KEY_ID ||
      !correlationId || !/^[a-zA-Z0-9._:-]{1,128}$/.test(correlationId)) return;
  let sequence = 0;
  const emit: ProviderDispatchTrace["emit"] = (phase, attempt, status) => {
    const monotonicMs = performance.now();
    try {
      write({ correlationId, attempt, phase, monotonicMs,
        timestampMs: performance.timeOrigin + monotonicMs, status });
    } catch {
      // Diagnostics must not change the outcome of the inference request.
    }
  };
  return { emit, nextAttempt: () => ++sequence, phase: (phase) => emit(phase, null) };
}

export function runWithProviderDispatchTrace<T>(trace: ProviderDispatchTrace, fetchProvider: () => T): T {
  subscribeToUndici();
  return tracing.run(trace, fetchProvider);
}
