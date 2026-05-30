from flask import Flask, request, jsonify
import requests
import json
import uuid
import time
import re
import random
import gc

app = Flask(__name__)

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
MAX_SESSIONS      = 100
MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10 MB
REQUEST_TIMEOUT   = 120               # seconds
SESSION_TTL       = 3600              # 1 hour


# ─────────────────────────────────────────────
# Session store (LRU eviction)
# ─────────────────────────────────────────────
class SessionStore:
    def __init__(self, max_size=MAX_SESSIONS):
        self.sessions     = {}
        self.access_times = {}
        self.max_size     = max_size

    def get(self, key):
        if key in self.sessions:
            self.access_times[key] = time.time()
            return self.sessions[key]
        return None

    def set(self, key, value):
        if len(self.sessions) >= self.max_size:
            oldest = min(self.access_times, key=self.access_times.get)
            self._evict(oldest)
        self.sessions[key]     = value
        self.access_times[key] = time.time()

    def _evict(self, key):
        data = self.sessions.pop(key, None)
        self.access_times.pop(key, None)
        if data and "session" in data:
            try:
                data["session"].close()
            except Exception:
                pass
        gc.collect()

    def cleanup(self):
        now     = time.time()
        expired = [k for k, t in self.access_times.items() if now - t > SESSION_TTL]
        for k in expired:
            self._evict(k)


store = SessionStore()


# ─────────────────────────────────────────────
# Session bootstrap
# ─────────────────────────────────────────────
def create_session() -> dict:
    session = requests.Session()
    headers = {
        "User-Agent":      "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36",
        "Accept":          "text/html,application/xhtml+xml",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        with session.get("https://www.perplexity.ai", headers=headers, timeout=30, stream=True) as resp:
            html_parts, total = [], 0
            for chunk in resp.iter_content(chunk_size=8192, decode_unicode=True):
                if chunk:
                    html_parts.append(chunk)
                    total += len(chunk)
                    if total >= 100 * 1024:
                        break
            html = "".join(html_parts)

        cookies = {c.name: c.value for c in session.cookies}

        version_m  = re.search(r'"version":"([\d.]+)"', html)
        version    = version_m.group(1) if version_m else "2.18"

        csrf_m     = re.search(r'csrf-token["\']?\s*[:=]\s*["\']([^"\']+)', html)
        csrf_token = csrf_m.group(1) if csrf_m else f"{uuid.uuid4().hex}%7C{uuid.uuid4().hex}"

        api_url_m  = re.search(r'"apiUrl":"([^"]+)"', html)
        api_url    = api_url_m.group(1) if api_url_m else "https://www.perplexity.ai/rest/sse/perplexity_ask"

        del html, html_parts
        gc.collect()

        return {
            "session":    session,
            "version":    version,
            "csrf_token": csrf_token,
            "api_url":    api_url,
            "created_at": int(time.time()),
        }
    except Exception:
        session.close()
        raise


# ─────────────────────────────────────────────
# Full answer extractor (reads entire SSE stream,
# returns one complete answer string + sources)
# ─────────────────────────────────────────────
def extract_answer(response_stream) -> tuple[str, list]:
    """
    Reads the full SSE stream and returns:
        (answer_text: str, sources: list)
    """
    answer  = ""
    sources = []
    total   = 0

    for raw_line in response_stream.iter_lines(decode_unicode=True):
        if not raw_line or not raw_line.startswith("data: "):
            continue

        total += len(raw_line)
        if total > MAX_RESPONSE_SIZE:
            break

        json_str = raw_line[6:].strip()
        if not json_str or json_str == "{}":
            continue

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            continue

        # Already found a good answer — skip further parsing
        if answer:
            continue

        # ── Path A: text-based FINAL step ────────────────────────────────
        if data.get("step_type") == "FINAL" and "text" in data:
            try:
                steps = json.loads(data["text"])
                if isinstance(steps, list):
                    for step in steps:
                        if step.get("step_type") == "FINAL":
                            raw_answer = step.get("content", {}).get("answer", "")
                            if raw_answer:
                                answer_data = json.loads(raw_answer)
                                answer      = answer_data.get("answer", "")
                                sources     = answer_data.get("web_results", [])
                            break
            except (json.JSONDecodeError, TypeError, KeyError):
                pass

        # ── Path B: blocks-based response ────────────────────────────────
        if not answer and "blocks" in data:
            for block in data.get("blocks", []):
                if block.get("intended_usage") in ("ask_text_0_markdown", "ask_text"):
                    answer = block.get("markdown_block", {}).get("answer", "")
                    if answer:
                        break

        # ── Path C: delta/incremental (fallback, concatenate) ─────────────
        if not answer:
            delta = data.get("delta") or data.get("answer_delta")
            if delta and isinstance(delta, str):
                answer += delta

    return answer.strip(), sources


# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status":  "ok",
        "service": "Perplexity Proxy API",
        "usage":   "/api/ask?prompt=your+question&mode=concise&model=turbo&search_focus=internet",
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":          "ok",
        "active_sessions": len(store.sessions),
        "timestamp":       int(time.time()),
    })


@app.route("/api/ask", methods=["GET"])
def ask():
    prompt = request.args.get("prompt", "").strip()
    if not prompt:
        return jsonify({"status": "error", "message": "prompt is required"}), 400

    mode         = request.args.get("mode",         "concise")
    model        = request.args.get("model",        "turbo")
    search_focus = request.args.get("search_focus", "internet")

    if random.random() < 0.01:
        store.cleanup()

    session_key = f"{mode}|{model}|{search_focus}"
    scraped = store.get(session_key)
    if not scraped:
        try:
            scraped = create_session()
            store.set(session_key, scraped)
        except Exception as exc:
            return jsonify({"status": "error", "message": f"Session init failed: {exc}"}), 502

    payload = {
        "query_str": prompt,
        "params": {
            "frontend_uuid":      str(uuid.uuid4()),
            "last_backend_uuid":  str(uuid.uuid4()),
            "read_write_token":   str(uuid.uuid4()),
            "mode":               mode,
            "model_preference":   model,
            "search_focus":       search_focus,
            "sources":            ["web"],
            "language":           "en-US",
            "timezone":           "UTC",
            "is_related_query":   False,
            "is_sponsored":       False,
            "is_incognito":       False,
            "local_search_enabled":            False,
            "use_schematized_api":             True,
            "send_back_text_in_streaming_api": False,
            "skip_search_enabled":             True,
            "always_search_override":          False,
            "override_no_search":              False,
            "should_ask_for_mcp_tool_confirmation": True,
            "prompt_source":   "user",
            "query_source":    "followup",
            "followup_source": "link",
            "source":          "mweb",
            "attachments":     [],
            "mentions":        [],
            "client_coordinates": None,
            "supported_features": ["browser_agent_permission_banner_v1.1"],
            "version":         scraped["version"],
        },
    }

    req_headers = {
        "User-Agent":   "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36",
        "Accept":       "text/event-stream",
        "Content-Type": "application/json",
        "x-request-id": str(uuid.uuid4()),
        "origin":       "https://www.perplexity.ai",
    }
    if scraped.get("csrf_token"):
        req_headers["x-csrf-token"] = scraped["csrf_token"]

    try:
        upstream = scraped["session"].post(
            scraped["api_url"],
            json=payload,
            headers=req_headers,
            stream=True,
            timeout=REQUEST_TIMEOUT,
        )
        upstream.raise_for_status()
    except requests.exceptions.RequestException as exc:
        store._evict(session_key)
        return jsonify({"status": "error", "message": f"Upstream error: {exc}"}), 502

    answer, sources = extract_answer(upstream)

    if not answer:
        return jsonify({"status": "error", "message": "No answer received from Perplexity"}), 500

    return jsonify({
        "status":    "ok",
        "answer":    answer,
        "sources":   sources,
        "prompt":    prompt,
        "mode":      mode,
        "model":     model,
        "timestamp": int(time.time()),
    })


@app.route("/api/clear_cache", methods=["POST"])
def clear_cache():
    before = len(store.sessions)
    store.cleanup()
    gc.collect()
    return jsonify({
        "status":    "ok",
        "evicted":   before - len(store.sessions),
        "remaining": len(store.sessions),
    })


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
