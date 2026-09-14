"""Explicit root selection; mounted providers and worker routing stay intact."""
import asyncio
from amplifier_loop_live_cli.catalog import MODELS, copilot_models, select_model, validate_controls, SelectionError

FAMILIES = {"provider-openai": "openai", "provider-anthropic": "anthropic",
            "provider-openai-chatgpt": "chatgpt", "provider-github-copilot": "copilot",
            "provider-gemini": "gemini"}


async def choices_for(providers, plan, *, discover=True):
    rows = []
    for spec in plan.get("providers", []):
        family = FAMILIES.get(spec["module"])
        identity = spec.get("id") or spec["module"].removeprefix("provider-")
        provider = providers.get(identity)
        if family is None or provider is None:
            continue
        choices = MODELS.get(family, ())
        error = None
        if family == "copilot" and discover:
            try:
                choices = copilot_models(await asyncio.wait_for(provider.list_models(), 45))
            except Exception as exc:
                error = type(exc).__name__
        info = provider.get_info()
        rows.append({"id": identity, "provider": family,
                     "model": info.defaults.get("model") or spec.get("config", {}).get("default_model"),
                     "effort": spec.get("config", {}).get("reasoning_effort"),
                     "models": [model.public() for model in choices], "discoveryError": error})
    return rows


class RootProvider:
    def __init__(self, provider, selection, family, runtime=None):
        self.provider, self.selection, self.family = provider, selection, family
        self.runtime = runtime
        self.native_bundle_live = bool(getattr(provider, "native_bundle_live", False) and
                                       selection["model"] == "gpt-6-astra")

    def __getattr__(self, name):
        return getattr(self.provider, name)

    def get_info(self):
        info = self.provider.get_info()
        return info.model_copy(update={"defaults": {**info.defaults, "model": self.selection["model"]}})

    async def complete(self, request, **kwargs):
        from amplifier_loop_live_cli.diagnostics import trace
        trace("provider.request", {"selection":self.selection,"request":request})
        if self.runtime:
            await self.runtime.emit("provider.request", **self.selection)
        updates = {"model": self.selection["model"], "reasoning_effort": self.selection["effort"]}
        # A bundle may have mounted an OpenAI-native computer definition while
        # another root provider is now selected. Keep its real tool/schema, but
        # use a portable function instead of forwarding a foreign wire type.
        if request.tools and self.family != "openai":
            from amplifier_core.message_models import ToolSpec
            updates["tools"] = [ToolSpec(name=tool.name, parameters=tool.parameters, description=tool.description)
                                if getattr(tool, "type", "function") != "function" else tool
                                for tool in request.tools]
        kwargs = {**kwargs, "model": updates["model"]}
        if updates["reasoning_effort"] is not None:
            kwargs["reasoning_effort"] = updates["reasoning_effort"]
        budget = self.selection.get("thinkingBudget")
        if budget is not None:
            updates["reasoning_effort"] = None
            kwargs.update(extended_thinking=budget > 0, thinking_budget_tokens=budget)
        if self.family in {"openai", "chatgpt"} and updates["reasoning_effort"] == "none":
            kwargs["reasoning"] = {"effort": "none"}
        try:
            response = await self.provider.complete(request.model_copy(update=updates), **kwargs)
            trace("provider.response", {"selection":self.selection,"response":response})
            return response
        except Exception as exc:
            trace("provider.error", {"selection":self.selection,"type":type(exc).__name__,"error":str(exc)})
            raise


async def select_root(session, plan, selection=None):
    providers = session.coordinator.get("providers") or {}
    choices = await choices_for(providers, plan)
    loop = session.coordinator.get("orchestrator")
    if selection:
        row = next((row for row in choices if row["id"] == selection.get("instance")), None)
        if row is None:
            raise SelectionError("Selected configured provider is unavailable")
        from amplifier_loop_live_cli.catalog import Model
        models = tuple(Model(**{**m, "efforts": tuple(m["efforts"])}) for m in row["models"])
        model = select_model(row["provider"], selection.get("model"), discovered=models)
        effort, budget, _ = validate_controls(model, selection.get("effort"), selection.get("thinkingBudget"))
        provider = providers[row["id"]]
        if row["provider"] == "openai" and model.id == "gpt-6-astra" and not getattr(provider, "native_bundle_live", False):
            from amplifier_loop_live_cli.bundle_astra import BundleAstraProvider
            provider = BundleAstraProvider.wrap(provider)
            session.coordinator.register_cleanup(provider.close_live)
        selection = {"instance": row["id"], "provider": row["provider"], "model": model.id,
                     "effort": effort, "thinkingBudget": budget}
        loop.root_provider = RootProvider(provider, selection, row["provider"], session.coordinator.get_capability("live.runtime"))
    return choices, selection
