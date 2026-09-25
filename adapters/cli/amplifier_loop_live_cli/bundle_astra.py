"""CLI image policy and ownership binding for provider-owned native transport."""
from amplifier_module_loop_live.scope import LIVE_OWNER, NATIVE_REQUEST as LOOP_NATIVE_REQUEST
from amplifier_module_provider_openai import OpenAIProvider
try:
    from amplifier_module_provider_openai.native import NativeResponsesProvider, NATIVE_REQUEST
    NATIVE_AVAILABLE = True
except ImportError:
    # An older configured provider remains a supported ordinary driver.
    NativeResponsesProvider = OpenAIProvider
    from amplifier_module_loop_live.scope import NATIVE_REQUEST
    NATIVE_AVAILABLE = False
from .image_budget import ImageBudgetMixin


class SafeOpenAIProvider(ImageBudgetMixin, OpenAIProvider):
    @classmethod
    def wrap(cls, original):
        provider=cls.__new__(cls)
        provider.__dict__=original.__dict__.copy()
        return provider

    def _convert_messages(self, messages, **kwargs):
        from amplifier_loop_live_cli.computer_results import failed_computer_history,validate_computer_outputs
        messages,_=failed_computer_history(messages)
        return validate_computer_outputs(super()._convert_messages(messages,**kwargs))

    async def complete(self, request, **kwargs):
        from amplifier_loop_live_cli.computer_results import ComputerResultError
        token = LOOP_NATIVE_REQUEST.set(None)
        try:
            return await super().complete(request,**kwargs)
        except ComputerResultError as exc:
            owner=LIVE_OWNER.get()
            if owner:
                await owner.runtime.emit("provider.error",error_type="computer_capture_failed",public_message=exc.public_message)
            raise
        finally:
            LOOP_NATIVE_REQUEST.reset(token)


class BundleAstraProvider(ImageBudgetMixin, NativeResponsesProvider):
    @classmethod
    def wrap(cls, original):
        if not NATIVE_AVAILABLE or "complete" in original.__dict__:
            return SafeOpenAIProvider.wrap(original)
        from .diagnostics import trace, trace_native_context
        return super().wrap(original, owner_getter=LIVE_OWNER.get, trace=trace,
                            trace_context=trace_native_context)

    def _prepare_native_messages(self, messages):
        from .computer_results import failed_computer_history
        return failed_computer_history(messages)[0]

    def _validate_native_items(self, items):
        from .computer_results import validate_computer_outputs
        return validate_computer_outputs(items)

    async def _native_response(self, params, **kwargs):
        # The provider owns native eligibility and its own request context.
        # Bind the loop's existing attribution only at the actual native wire
        # boundary, so its durable job ledger can retain original async calls.
        token = LOOP_NATIVE_REQUEST.set(self if NATIVE_REQUEST.get() is self else None)
        try:
            return await super()._native_response(params, **kwargs)
        finally:
            LOOP_NATIVE_REQUEST.reset(token)

    async def complete(self, request, **kwargs):
        from .computer_results import ComputerResultError
        # A nested utility/ordinary call must not inherit its caller's native
        # job attribution. The native boundary below opts back in only when used.
        token = LOOP_NATIVE_REQUEST.set(None)
        try:
            return await super().complete(request, **kwargs)
        except ComputerResultError as exc:
            owner = LIVE_OWNER.get()
            if owner:
                await owner.runtime.emit("provider.error", error_type="computer_capture_failed",
                                         public_message=exc.public_message)
            raise
        finally:
            LOOP_NATIVE_REQUEST.reset(token)


async def install(coordinator, native=True):
    from amplifier_loop_live_cli.consultation_images import ImageChatGPTProvider
    from amplifier_module_provider_openai_chatgpt import ChatGPTProvider
    installed = []
    for name, provider in (coordinator.get("providers") or {}).items():
        if type(provider) is OpenAIProvider:
            wrapped = (BundleAstraProvider if native and provider.default_model == "gpt-6-astra" and "complete" not in provider.__dict__ else SafeOpenAIProvider).wrap(provider)
            await coordinator.mount("providers", wrapped, name=name)
            if isinstance(wrapped,BundleAstraProvider):installed.append(name)
        elif type(provider) is ChatGPTProvider:
            await coordinator.mount("providers", ImageChatGPTProvider.wrap(provider), name=name)
    return installed
