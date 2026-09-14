"""Optional public execution evidence. No protocol mutation or replay."""
import contextvars
import json
import os
from pathlib import Path

SINK = contextvars.ContextVar("converge_debug_sink", default=None)


def enabled():
    path = os.environ.get("CONVERGE_DIAGNOSTICS_CONFIG")
    if not path: return False
    try:
        from datetime import datetime, timezone
        config = json.loads(Path(path).read_text())
        expiry = config.get("expiresAt")
        return config.get("enabled") is True and (not expiry or datetime.fromisoformat(expiry.replace("Z", "+00:00")) > datetime.now(timezone.utc))
    except Exception: return False


def public(value, depth=0):
    if depth > 10: return "[DEPTH LIMIT]"
    if hasattr(value, "model_dump"): value = value.model_dump()
    if isinstance(value, dict):
        kind = str(value.get("type", ""))
        if kind in {"image", "input_image"} or (kind=="base64" and str(value.get("media_type", "")).startswith("image/")):
            return {"type":kind,"content":"[IMAGE OMITTED]"}
        if kind in {"thinking", "reasoning", "redacted_thinking"} or (kind.startswith("response.") and any(s in kind for s in ("reasoning", "thinking"))) or value.get("channel") == "analysis":
            return {"type": value.get("type"), "content": "[HIDDEN REASONING OMITTED]"}
        return {k: "[OMITTED]" if k in {"encrypted_content", "reasoning_content", "thinking", "signature"} else public(v, depth+1) for k,v in list(value.items())[:300]}
    if isinstance(value, (list,tuple)): return [public(v, depth+1) for v in value[:300]]
    if isinstance(value, str): return value[:24000] + ("[TRUNCATED]" if len(value)>24000 else "")
    if value is None or isinstance(value,(bool,int,float)): return value
    return str(value)[:2000]


def trace(kind, data):
    sink = SINK.get()
    if sink and enabled():
        try:
            from amplifier_loop_live_cli.configured import redacted
            safe = redacted(public(data))
            encoded = json.dumps(safe, ensure_ascii=True, default=str)
            if len(encoded) > 160000:
                safe = {"truncated": True, "originalBytes": len(encoded), "preview": encoded[:24000]}
            sink(kind, safe)
        except Exception: pass  # Diagnostics cannot fail work.


def trace_native_context(payload):
    """Keep wire order inspectable even when large request bodies are truncated.

    Only structural identities are copied, never message text, tool arguments,
    results, images or reasoning. Pages keep long sessions inside event limits.
    """
    if not SINK.get() or not enabled(): return
    items = payload.get("input", [])
    if not isinstance(items, list): return
    for start in range(0, max(1, len(items)), 100):
        trace("native.context", {"previous_response_id": payload.get("previous_response_id"),
            "input_items": len(items), "offset": start,
            "items": [{"index": i, "item_type": item.get("type", "message"), **{k: item[k] for k in
                ("role", "id", "call_id", "name", "phase", "async") if k in item}}
                for i, item in enumerate(items[start:start+100], start) if isinstance(item, dict)]})


def install_hooks(coordinator):
    if getattr(coordinator, "_converge_debug_hooks", False): return
    coordinator._converge_debug_hooks = True
    from amplifier_core import HookResult
    async def observe(event, data):
        trace("hook."+event, {"engineSessionId":getattr(coordinator,"session_id",None),"event":data})
        return HookResult()
    for event in ("provider:request", "provider:response", "provider:error", "tool:pre", "tool:post", "tool:error",
                  "session:start", "session:end", "session:fork", "session:resume", "orchestrator:complete"):
        coordinator.hooks.register(event, observe, name="converge-debug-"+event)
