"""Bounded public turn failures; exception payloads remain private causes."""

import re

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


def count_diagnostic(error):
    """Recognize an optional provider contract without importing its SDK."""
    if (type(error).__module__, type(error).__name__) != (
            'amplifier_module_provider_openai._token_count', 'TokenCountError'):
        return None
    value = getattr(error, 'count_failure', None)
    if not isinstance(value, dict):
        return {}
    result = {}
    if isinstance(value.get('category'), str) and value['category'] in {'timeout', 'connection', 'rate_limit', 'service',
                                'authentication', 'permission', 'quota', 'invalid_request', 'invalid_response'}:
        result['category'] = value['category']
    result['retryable'] = value.get('retryable') is True and result.get('category') in {
        'timeout', 'connection', 'rate_limit', 'service'}
    for key, low, high in [('httpStatus', 400, 599), ('attempts', 1, 3)]:
        number = value.get(key)
        if type(number) is int and low <= number <= high:
            result[key] = number
    request_id = value.get('requestId')
    if isinstance(request_id, str) and re.fullmatch(r'[A-Za-z0-9_-]{1,100}', request_id):
        result['requestId'] = request_id
    return result


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
    compaction = (type(error).__module__ == "amplifier_module_context_managed.errors"
                  and type(error).__name__ == "CompactionError")
    code = getattr(error, "code", None) if compaction else None
    safe_codes = {"native_input_oversized", "native_checkpoint_invalid", "native_no_reduction",
                  "native_measurement_unavailable", "native_compaction_failed",
                  "disabled", "request_context_unavailable", "invalid_native_contract",
                  "authoritative_measurement_unavailable"}
    count_failure = count_diagnostic(error)
    if count_failure is not None:
        category, stage = 'context_measurement', 'context_preparation'
        message = 'Could not check conversation size. Saved history and checkpoint are preserved.'
    elif compaction:
        category, stage = "context_compaction", "context_preparation"
        message = "Context compaction failed. Original history is preserved; repair context preparation before continuing."
    elif isinstance(error, ContextLengthError):
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
    return {**({"error_code": code} if code in safe_codes else {}),
            **({'count_failure': count_failure} if count_failure is not None else {}),
            "error_type": "CompactionError" if compaction else kind, "error_category": category, "error_stage": stage,
            "error_message": message, "effects": "not_rolled_back", "replayed": False,
            "retryable": (count_failure.get('retryable', False) if count_failure is not None
                          else isinstance(error, LLMError) and error.retryable is True)}


class ManagerTurnError(RuntimeError):
    """Safe public message with the original exception available via __cause__."""

    def __init__(self, failure):
        self.failure = failure
        super().__init__(failure["error_message"] + " Work was not automatically replayed; earlier effects were not rolled back.")
