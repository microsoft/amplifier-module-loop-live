"""Apply workspace worker choices via the existing delegate preference contract."""
import copy
import json
from pathlib import Path


class WorkerSettings:
    def __init__(self, path=None, families=None):
        self.path = Path(path) if path else None
        self.families = families or {}

    def snapshot(self):
        if self.path is None or not self.path.exists():
            return {"version": 1, "revision": 0, "defaultRoute": None, "agents": {}}
        value = json.loads(self.path.read_text())
        if value.get("version") != 1 or not isinstance(value.get("revision"), int):
            raise ValueError("Unsupported saved worker settings")
        return value

    def apply(self, input):
        value = self.snapshot()
        agent = input.get("agent", "self")
        provenance = {"source": "bundle", "revision": value["revision"], "agent": agent}
        # Resume belongs to the saved child. A new workspace default never
        # repins its provider; explicit caller resume preferences still work.
        if input.get("session_id"):
            return input, {**provenance, "source": "explicit_resume" if input.get("provider_preferences") or input.get("model_role") else "saved_session"}
        if agent in value["agents"]:
            route = value["agents"][agent]
            provenance["source"] = "agent_override" if route else "agent_bundle_default"
        elif input.get("provider_preferences") or input.get("model_role"):
            return input, {**provenance, "source": "explicit_delegation", "modelRole": input.get("model_role")}
        else:
            route = value["defaultRoute"]
            if route:
                provenance["source"] = "workspace_default"
        if not route:
            if provenance["source"]=="agent_bundle_default":
                return {k:v for k,v in input.items() if k not in {"provider_preferences","model_role"}},provenance
            return input, provenance
        candidates = route.get("candidates")
        if not isinstance(candidates, list) or not 1 <= len(candidates) <= 4:
            raise ValueError("Invalid saved worker candidates")
        preferences = []
        for choice in candidates:
            instance, model = choice["instance"], choice["model"]
            # Only behavioral knobs enter the normal preference contract.
            # The authenticated host validates choices before saving. Missing
            # instances stay explicit fallback candidates for the CLI resolver.
            if not isinstance(instance, str) or not isinstance(model, str):
                raise ValueError("Invalid saved worker selection")
            effort, budget = choice.get("effort"), choice.get("thinkingBudget")
            cfg = {}
            if budget is not None:
                cfg.update(reasoning_effort=None, extended_thinking=budget > 0, thinking_budget_tokens=budget)
            elif effort is not None:
                if self.families.get(instance) == "gemini":
                    cfg["extra_request_params"] = {"thinking_config": {"thinking_level": effort}}
                else:
                    cfg["reasoning_effort"] = effort
            preferences.append({"provider": instance, "model": model, "config": cfg})
        configured = {**input, "provider_preferences": preferences}
        configured.pop("model_role", None)
        return configured, {**provenance, "candidates": copy.deepcopy(candidates)}


def effective_selection(provider, providers, request=None, kwargs=None):
    """Public behavioral settings at the selected driver's call boundary."""
    kwargs = kwargs or {}
    info = provider.get_info()
    config = getattr(provider, "config", None) or getattr(provider, "_config", None) or {}
    if not isinstance(config, dict):
        config = {}
    defaults = getattr(info,"defaults",None) or {}
    def selected(key, fallback=None):
        value = kwargs.get(key)
        if value is None and request is not None:
            value = getattr(request, key, None)
        return value if value is not None else config.get(key, fallback)
    effort=selected("reasoning_effort")
    if getattr(info,"id",None) in {"gemini","google","provider-gemini"}:
        thinking=(config.get("extra_request_params") or {}).get("thinking_config") or {}
        effort=thinking.get("thinking_level",effort)
    return {"instance": next((name for name, mounted in providers.items() if mounted is provider), None),
            "provider": getattr(info,"id",None), "model": selected("model", config.get("default_model") or defaults.get("model")),
            "effort": effort, "thinkingBudget": selected("thinking_budget_tokens"),
            "source": "provider_request" if request is not None else "mounted_provider"}


class ObservedWorkerProvider:
    def __init__(self, original, providers, registry, identity):
        self.original, self.providers, self.registry, self.identity = original, providers, registry, identity

    def __getattr__(self, name):
        return getattr(self.original, name)

    async def complete(self, request, **kwargs):
        selected = effective_selection(self.original, self.providers, request, kwargs)
        self.registry.configuration(self.identity, selected)
        if request.tools and selected["provider"] not in {"openai","provider-openai"}:
            from amplifier_core.message_models import ToolSpec
            request=request.model_copy(update={"tools":[ToolSpec(name=t.name,parameters=t.parameters,description=t.description)
                if getattr(t,"type","function")!="function" else t for t in request.tools]})
        return await self.original.complete(request, **kwargs)
