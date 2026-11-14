"""Explicit root selection; mounted providers and worker routing stay intact."""
import asyncio
import copy
import inspect
from amplifier_loop_live_cli.catalog import MODELS, copilot_models, select_model, validate_controls, SelectionError

FAMILIES = {"provider-openai": "openai", "provider-anthropic": "anthropic",
            "provider-openai-chatgpt": "chatgpt", "provider-github-copilot": "copilot",
            "provider-gemini": "gemini"}


def optional_request_call(method, request, *, request_options=None, **kwargs):
    """Preserve strict optional signatures without retrying implementation errors."""
    parameters = inspect.signature(method).parameters
    if "request_options" in parameters or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        kwargs["request_options"] = request_options
    elif request_options:
        # These two selected values are already explicit on the canonical view.
        remaining = {key: value for key, value in request_options.items()
                     if key not in {"model", "reasoning_effort"} or getattr(request, key, None) != value}
        if remaining:
            return None  # Counting these options is genuinely unavailable.
    return method(request, **kwargs)


def accepted_preparation(result, accept):
    """Commit cleanup identity only after an actual preparation succeeds."""
    if inspect.isawaitable(result):
        async def wait():
            value = await result
            accept()
            return value
        return wait()
    accept()
    return result


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
        self._request_candidate = None
        self._media_release_candidate = None
        self._install_request_methods()

    def _install_request_methods(self):
        # Optional methods must remain genuinely optional, and must apply the
        # same selected model/options before private projection as complete().
        for name in ("request_budget", "refit_request", "stream", "compact_context", "media_budget_witness",
                     "media_text_fallback_witness", "release_media_request"):
            if callable(getattr(self.provider, name, None)):
                setattr(self, name, getattr(self, "_" + name))
        if (callable(getattr(self.provider, "stream", None))
                and callable(getattr(self.provider, "get_stream_request_support", None))):
            self.get_stream_request_support = self._get_stream_request_support

    def _request_view(self, request, kwargs):
        updates = {"model": self.selection["model"], "reasoning_effort": self.selection["effort"]}
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
        snapshot = request.model_dump()
        cached = self._request_candidate
        if cached is not None and cached[0] is request and cached[1] == snapshot and cached[2] == updates:
            return cached[3], kwargs
        view = request.model_copy(deep=True, update=updates)
        # Only canonical refs live here. Keep the same canonical selected view
        # across count and send so the provider can bind its private projection.
        self._request_candidate = (request, copy.deepcopy(snapshot), copy.deepcopy(updates), view)
        return view, kwargs

    def _request_budget(self, request, *, request_options=None, **kwargs):
        view, options, accept = self._prepare_request_view(request, request_options)
        return accepted_preparation(optional_request_call(
            self.provider.request_budget, view, request_options=options, **kwargs), accept)

    def _refit_request(self, request, *, request_options=None, **kwargs):
        view, options, accept = self._prepare_request_view(request, request_options)
        return accepted_preparation(optional_request_call(
            self.provider.refit_request, view, request_options=options, **kwargs), accept)

    def _prepare_request_view(self, request, request_options):
        caller_options = copy.deepcopy(request_options or {})
        view, options = self._request_view(request, caller_options)
        candidate = (request, view, caller_options, copy.deepcopy(options))
        def accept():
            self._media_release_candidate = candidate
        return view, options, accept

    def _stream(self, request, **kwargs):
        request, kwargs = self._request_view(request, kwargs)
        return self.provider.stream(request, **kwargs)

    def _compact_context(self, request, **kwargs):
        request, kwargs = self._request_view(request, kwargs)
        return self.provider.compact_context(request, **kwargs)

    def _media_budget_witness(self, request, *, request_options=None):
        view, options, accept = self._prepare_request_view(request, request_options)
        return accepted_preparation(self.provider.media_budget_witness(view, request_options=options), accept)

    def _media_text_fallback_witness(self, request, *, request_options=None):
        view, options, accept = self._prepare_request_view(request, request_options)
        return accepted_preparation(self.provider.media_text_fallback_witness(view, request_options=options), accept)

    def _release_media_request(self, request, *, request_options=None):
        candidate = self._media_release_candidate
        if candidate is None or candidate[0] is not request or candidate[2] != (request_options or {}):
            return False
        # Cleanup is an identity lookup, never a new selection/conversion. The
        # caller may have mutated its request or changed selection since count.
        result = self.provider.release_media_request(candidate[1], request_options=copy.deepcopy(candidate[3]))
        if result is True:
            self._media_release_candidate = None
        return result

    def _get_stream_request_support(self, *, model, request_options=None):
        options = dict(request_options or {})
        options["model"] = self.selection["model"]
        effort, budget = self.selection["effort"], self.selection.get("thinkingBudget")
        if effort is not None:
            options["reasoning_effort"] = effort
        if budget is not None:
            options.update(extended_thinking=budget > 0, thinking_budget_tokens=budget)
        if self.family in {"openai", "chatgpt"} and effort == "none" and budget is None:
            options["reasoning"] = {"effort": "none"}
        return self.provider.get_stream_request_support(
            model=self.selection["model"], request_options=options)

    def __getattr__(self, name):
        if name in {"get_stream_request_support", "release_media_request"}:
            raise AttributeError(name)
        return getattr(self.provider, name)

    def get_info(self):
        info = self.provider.get_info()
        return info.model_copy(update={"defaults": {**info.defaults, "model": self.selection["model"]}})

    async def complete(self, request, **kwargs):
        from amplifier_loop_live_cli.diagnostics import trace
        trace("provider.request", {"selection":self.selection,"request":request})
        if self.runtime:
            await self.runtime.emit("provider.request", **self.selection)
        # A bundle may have mounted an OpenAI-native computer definition while
        # another root provider is now selected. Keep its real tool/schema, but
        # use a portable function instead of forwarding a foreign wire type.
        request, kwargs = self._request_view(request, kwargs)
        try:
            response = await self.provider.complete(request, **kwargs)
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
            cleanup = provider.close_live if getattr(provider, "native_bundle_live", False) else provider.close
            session.coordinator.register_cleanup(cleanup)
        selection = {"instance": row["id"], "provider": row["provider"], "model": model.id,
                     "effort": effort, "thinkingBudget": budget}
        loop.root_provider = RootProvider(provider, selection, row["provider"], session.coordinator.get_capability("live.runtime"))
    return choices, selection
