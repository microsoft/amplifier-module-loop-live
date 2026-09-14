"""Model/effort choices checked against official docs on 2026-09-12."""

from dataclasses import dataclass, asdict


class SelectionError(ValueError):
    """Safe, user-facing selection error; distinct from provider payload errors."""


@dataclass(frozen=True)
class Model:
    alias: str
    id: str
    efforts: tuple[str, ...]
    default_effort: str | None
    thinking_budget: bool = False

    def public(self):
        return asdict(self)


OPENAI_EFFORTS = ("none", "low", "medium", "high", "xhigh", "max")
REASONING_EFFORTS = ("low", "medium", "high", "xhigh", "max")
MODELS = {
    "openai": (
        Model("astra", "gpt-6-astra", REASONING_EFFORTS, "low"),
        Model("terra", "gpt-5.6-terra", OPENAI_EFFORTS, "medium"),
        Model("sol", "gpt-5.6-sol", OPENAI_EFFORTS, "medium"),
        Model("luna", "gpt-5.6-luna", OPENAI_EFFORTS, "medium"),
    ),
    "anthropic": (
        Model("fable", "claude-fable-5-1", REASONING_EFFORTS, "high"),
        Model("opus", "claude-opus-5", REASONING_EFFORTS, "high"),
        Model("sonnet", "claude-sonnet-5", REASONING_EFFORTS, "high"),
        Model("haiku", "claude-haiku-4-5-20251001", (), None, thinking_budget=True),
    ),
    "gemini": (Model("flash", "gemini-3.8-flash", ("low", "medium", "high"), "medium"),),
}
MODELS["chatgpt"] = MODELS["openai"]
PROVIDERS = ("openai", "anthropic", "chatgpt", "copilot", "gemini")
DEFAULTS = {"openai": "astra", "anthropic": "sonnet", "chatgpt": "astra", "gemini": "flash"}


def provider_alias(name):
    return {"astra": "openai", "openai-chatgpt": "chatgpt", "github-copilot": "copilot"}.get(name, name)


def select_model(provider, model=None, *, discovered=()):
    provider = provider_alias(provider)
    choices = discovered if provider == "copilot" else MODELS.get(provider, ())
    if not choices:
        raise SelectionError("No model catalog available for " + provider)
    if model is None:
        model = DEFAULTS.get(provider, "gpt-5.6-sol")
        if provider == "copilot" and not any(m.id == model for m in choices):
            return choices[0]
    for choice in choices:
        if model in {choice.alias, choice.id}:
            return choice
    if provider == "copilot":
        if model == "haiku":
            return select_model(provider, "claude-haiku-4.5", discovered=discovered)
        for family in ("openai", "anthropic", "gemini"):
            match = next((m for m in MODELS[family] if m.alias == model), None)
            if match:
                return select_model(provider, match.id, discovered=discovered)
    raise SelectionError(f"Unknown {provider} model '{model}'; use --list-models")


def validate_controls(model, effort=None, thinking_budget=None, max_output_tokens=None):
    effort = model.default_effort if effort is None else effort
    if effort is not None and effort not in model.efforts:
        allowed = ", ".join(model.efforts) or ("no named effort; use --thinking-budget" if
                    model.thinking_budget else "no effort control advertised by this provider")
        raise SelectionError(f"{model.id} supports {allowed}")
    if thinking_budget is not None:
        if not model.thinking_budget:
            raise SelectionError("--thinking-budget is only supported by Haiku in this catalog")
        if thinking_budget != 0 and not 1024 <= thinking_budget <= 32000:
            raise SelectionError("Thinking budget must be 0 (off) or 1024–32000 tokens")
    elif model.thinking_budget:
        thinking_budget = 0
    # Reasoning shares the output budget; an explicit cap is never silently enlarged.
    tokens = max_output_tokens if max_output_tokens is not None else (
        65536 if effort in {"high", "xhigh", "max"} else 16384)
    if type(tokens) is not int or not 2048 <= tokens <= 65536:
        raise SelectionError("--max-output-tokens must be 2048–65536")
    if thinking_budget and thinking_budget >= tokens:
        if max_output_tokens is not None:
            raise SelectionError("Output limit must exceed the thinking budget")
        tokens = thinking_budget + 4096
    return effort, thinking_budget, tokens


def copilot_models(infos):
    return tuple(Model(m.id, m.id, tuple(m.defaults.get("supported_reasoning_efforts") or ()),
                       m.defaults.get("reasoning_effort")) for m in infos
                 if "tools" in m.capabilities)
