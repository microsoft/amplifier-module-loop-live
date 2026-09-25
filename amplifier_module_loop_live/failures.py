"""Bounded public turn failures; exception payloads remain private causes."""

from amplifier_core.llm_errors import (
    AuthenticationError,
    ContentFilterError,
    ContextLengthError,
    InvalidRequestError,
    LLMError,
    LLMTimeoutError,
    ProviderUnavailableError,
    RateLimitError,
)


def turn_failure(error, stage="manager_turn"):
    """Describe a failure without copying SDK bodies, prompts, paths or keys.

    A provider attribute identifies a provider-related check, not proof that a
    request reached the service. Earlier tools may already have changed state.
    """
    category, message = "unknown", "Manager turn failed. Inspect saved details before continuing."
    origin = error.__traceback__
    while origin is not None and origin.tb_next is not None:
        origin = origin.tb_next
    module = origin.tb_frame.f_globals.get("__name__", "") if origin else ""
    local_context = (module.startswith("amplifier_module_context_")
                     or module == "amplifier_module_loop_streaming")
    if isinstance(error, ContextLengthError):
        category = "context_limit"
        if local_context:
            stage = "context_preparation"
            message = "Local context preparation could not fit the required content within the input budget. Required content was not discarded."
        else:
            message = "The request could not fit within the context budget."
    elif isinstance(error, AuthenticationError):
        category, message = "authentication", "The selected provider rejected its credentials."
    elif isinstance(error, RateLimitError):
        category, message = "rate_limit", "The selected provider's rate limit was reached."
    elif isinstance(error, ContentFilterError):
        category, message = "content_filter", "The selected provider stopped the request under its content policy."
    elif isinstance(error, InvalidRequestError):
        category, message = "invalid_request", "The selected provider could not accept the request format."
    elif isinstance(error, LLMTimeoutError):
        category, message = "provider_timeout", "The provider request timed out; its outcome may be unknown."
    elif isinstance(error, ProviderUnavailableError):
        category, message = "provider_unavailable", "The selected provider is unavailable."
    if isinstance(error, LLMError) and error.provider and stage != "context_preparation":
        stage = "provider_request"
    # Class names from arbitrary third-party exceptions are not trusted text.
    kind = next((cls.__name__ for cls in type(error).__mro__
                 if cls.__module__ in {"builtins", "amplifier_core.llm_errors"}), "Exception")
    return {"error_type": kind, "error_category": category, "error_stage": stage,
            "error_message": message, "effects": "not_rolled_back", "replayed": False,
            "retryable": isinstance(error, LLMError) and error.retryable is True}


class ManagerTurnError(RuntimeError):
    """Safe public message with the original exception available via __cause__."""

    def __init__(self, failure):
        self.failure = failure
        super().__init__(failure["error_message"] + " Work was not automatically replayed; earlier effects were not rolled back.")
