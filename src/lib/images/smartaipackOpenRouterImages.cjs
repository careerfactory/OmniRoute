"use strict";

const endpoint = "https://openrouter.ai/api/v1/images";
const parameters = [
  "model",
  "prompt",
  "resolution",
  "aspect_ratio",
  "size",
  "quality",
  "n",
  "response_format",
];

function error(status, code) {
  return Response.json({ error: { code } }, { status });
}

// Try configured keys only after an explicit authentication rejection.
async function fetchImage(payload, keys, signal, fetchImpl) {
  let response;
  for (const key of keys) {
    response = await fetchImpl(endpoint, {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      // OmniRoute's proxy fetch treats a stream as non-replayable.
      body: new Blob([payload]).stream(),
      duplex: "half",
      redirect: "error",
      signal,
    });
    if (response.status !== 401) return response;
    await response.body?.cancel();
  }
  return response;
}

// Called by the private SmartAIPack management proxy. Keep the provider key in OmniRoute.
async function forwardOpenRouterImage(body, credentials, signal, fetchImpl = fetch) {
  const primary = credentials?.apiKey || credentials?.accessToken;
  const data = credentials?.providerSpecificData;
  const extra = Array.isArray(data?.extraApiKeys) ? data.extraApiKeys : [];
  const candidates =
    data?.apiKeyHealth?.primary?.status === "invalid" ? [...extra, primary] : [primary, ...extra];
  const keys = [...new Set(candidates.filter((key) => typeof key === "string" && key.trim()))];
  if (!keys.length) return error(503, "openrouter_images_unavailable");
  if (
    typeof body?.model !== "string" ||
    !/^[a-z0-9.-]+\/[a-z0-9.-]+$/.test(body.model) ||
    typeof body.prompt !== "string" ||
    body.n !== 1
  ) {
    return error(400, "invalid_image_request");
  }

  const payload = Object.fromEntries(
    parameters.flatMap((parameter) =>
      Object.hasOwn(body, parameter) ? [[parameter, body[parameter]]] : []
    )
  );
  try {
    const deadline = AbortSignal.any([signal, AbortSignal.timeout(115_000)]);
    const response = await fetchImage(JSON.stringify(payload), keys, deadline, fetchImpl);
    if (!response.ok) {
      if (response.status !== 401) await response.body?.cancel();
      if (response.status === 429) return error(429, "openrouter_images_rate_limited");
      if ([400, 403, 422].includes(response.status)) return error(422, "image_request_rejected");
      return error(502, "openrouter_images_failed");
    }
    return new Response(response.body, {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "X-SmartAIPack-Images": "1",
      },
    });
  } catch (cause) {
    const timeout = cause?.name === "TimeoutError" || cause?.name === "AbortError";
    return error(
      timeout ? 504 : 502,
      timeout ? "openrouter_images_timeout" : "openrouter_images_failed"
    );
  }
}

module.exports = { forwardOpenRouterImage };
