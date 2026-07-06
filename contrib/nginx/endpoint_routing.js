/**
 * OmniRoute Endpoint-Aware Model Routing (njs module)
 *
 * Rewrites the model prefix based on the client endpoint:
 *   /v1/responses        → main/<model>  (→ KFC, responses provider)
 *   /v1/chat/completions → old/<model>   (→ KFC-Old, chat provider)
 *
 * Handles all client input forms:
 *   "gpt-5.5-pro"       → adds correct prefix for the endpoint
 *   "main/gpt-5.5-pro"  → rewrites prefix if endpoint doesn't match
 *   "old/gpt-5.5-pro"   → rewrites prefix if endpoint doesn't match
 *
 * Models that belong to OTHER providers (e.g. gemini-cli) are
 * NOT rewritten — they pass through to OmniRoute alias resolution.
 *
 * Does NOT modify the request body — sets the X-Route-Model header
 * via js_var + internalRedirect. OmniRoute reads this header in
 * resolveRoutingModel() and uses it for provider routing.
 * SSE response streaming is fully preserved.
 */

var KNOWN_PREFIXES = ["main/", "old/"];

// Model name prefixes that belong to OTHER providers (not KFC/KFC-Old).
// These models must NOT be rewritten — let OmniRoute handle them
// via aliases or direct provider/model resolution.
var SKIP_MODEL_PREFIXES = [
    "gemini-",
    "gemini/",
    "gemini-cli/",
];

function stripPrefix(model) {
    for (var i = 0; i < KNOWN_PREFIXES.length; i++) {
        if (model.indexOf(KNOWN_PREFIXES[i]) === 0) {
            return model.substring(KNOWN_PREFIXES[i].length);
        }
    }
    return model;
}

function prefixForEndpoint(uri) {
    if (uri.indexOf("/responses") !== -1) {
        return "main/";
    }
    return "old/";
}

function shouldSkipModel(model) {
    // If model already has a provider prefix (contains "/") and it's
    // NOT one of our known KFC prefixes, don't touch it
    var slashIdx = model.indexOf("/");
    if (slashIdx !== -1) {
        var prefix = model.substring(0, slashIdx + 1);
        var isKfcPrefix = false;
        for (var i = 0; i < KNOWN_PREFIXES.length; i++) {
            if (prefix === KNOWN_PREFIXES[i]) {
                isKfcPrefix = true;
                break;
            }
        }
        if (!isKfcPrefix) {
            return true;
        }
    }

    // Check bare model names that belong to other providers
    for (var j = 0; j < SKIP_MODEL_PREFIXES.length; j++) {
        if (model.indexOf(SKIP_MODEL_PREFIXES[j]) === 0) {
            return true;
        }
    }

    return false;
}

/**
 * js_content handler: reads request body, computes the correct
 * prefixed model name, stores it in $route_model (js_var),
 * then does an internal redirect to the proxy location.
 * The original request body is preserved across the redirect.
 *
 * For OPTIONS requests (CORS preflight), responds directly.
 * For non-KFC models (gemini, etc.), proxies without rewrite.
 */
function handleRequest(r) {
    // Handle CORS preflight
    if (r.method === "OPTIONS") {
        r.headersOut["Access-Control-Allow-Origin"] = "*";
        r.headersOut["Access-Control-Allow-Methods"] =
            "GET, POST, OPTIONS, PUT, DELETE, PATCH";
        r.headersOut["Access-Control-Allow-Headers"] =
            "Authorization, Content-Type, Accept, Origin, User-Agent, X-Requested-With";
        r.headersOut["Access-Control-Max-Age"] = "1728000";
        r.headersOut["Content-Type"] = "text/plain; charset=utf-8";
        r.return(204);
        return;
    }

    // For non-POST methods, proxy without model rewrite
    if (r.method !== "POST") {
        r.internalRedirect("/_omni_proxy" + r.uri);
        return;
    }

    // Parse body and rewrite model prefix
    var body;
    try {
        body = JSON.parse(r.requestText || "{}");
    } catch (e) {
        r.internalRedirect("/_omni_proxy" + r.uri);
        return;
    }

    var model = body.model;
    if (model && typeof model === "string" && !shouldSkipModel(model)) {
        var bare = stripPrefix(model);
        var prefix = prefixForEndpoint(r.uri);
        r.variables.route_model = prefix + bare;
    }

    r.internalRedirect("/_omni_proxy" + r.uri);
}

export default { handleRequest };
