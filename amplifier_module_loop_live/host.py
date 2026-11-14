"""Host extension points. The default needs only Amplifier's coordinator."""

import copy
import inspect
from collections.abc import Mapping


async def attachment_content(host, command, coordinator):
    """Accept synchronous legacy encoders and asynchronous structured encoders."""
    value = host.content(command, coordinator)
    return await value if inspect.isawaitable(value) else value


def attachment_message(message, encoded):
    """Merge an explicit host-owned carrier; never parse attachment text."""
    if not isinstance(encoded, Mapping):
        return {**message, "content": copy.deepcopy(encoded)}
    if set(encoded) != {"content", "metadata"} or not isinstance(encoded["metadata"], Mapping):
        raise ValueError("Invalid structured attachment content")
    metadata = message.get("metadata") or {}
    if not isinstance(metadata, Mapping):
        raise ValueError("Invalid message metadata")
    supplied = encoded["metadata"]
    if any(key in metadata and metadata[key] != value for key, value in supplied.items()):
        raise ValueError("Attachment metadata conflicts with message provenance")
    # Input identity and authority belong to the admitted command, never an
    # encoder. Other namespaces, including ordered refs, remain host-owned data.
    if any(key in supplied and key not in metadata for key in ("amplifier_input", "live_job_result")):
        raise ValueError("Attachment encoder cannot introduce input provenance")
    return {**message, "content": copy.deepcopy(encoded["content"]),
            "metadata": {**copy.deepcopy(dict(supplied)), **copy.deepcopy(dict(metadata))}}


async def checkpoint_attachment(message, coordinator):
    if (message.get("metadata") or {}).get("experimental.media.v1"):
        from amplifier_module_loop_streaming.media import checkpoint_settled_results
        await checkpoint_settled_results(coordinator, has_media=True)


class HostAdapter:
    async def prepare_execution(self, loop, coordinator, providers):
        runtime = coordinator.get_capability("live.runtime") if coordinator else None
        return runtime, providers, None

    def finite_finished(self, scope, coordinator, status, result=""):
        pass

    def prepare_provider(self, provider, coordinator):
        """Return the selected view; legacy mutation-only hosts may return None."""
        return provider

    def content(self, command, coordinator):
        encode = coordinator.get_capability("live.attachments.encode") if coordinator else None
        if encode is None:
            raise ValueError("This host does not support work attachments")
        return encode(command, coordinator)


class AttachmentContext:
    """Preserve prompt hooks while replacing the corresponding user message."""

    def __init__(self, context, prompt, content, coordinator=None):
        self.context, self.prompt, self.content = context, prompt, content
        self.coordinator = coordinator
        self.pending = True

    def __getattr__(self, name):
        return getattr(self.context, name)

    async def add_message(self, message):
        if self.pending and message.get("role") == "user" and message.get("content") == self.prompt:
            message = attachment_message(message, self.content)
            self.pending = False
            result = await self.context.add_message(message)
            await checkpoint_attachment(message, self.coordinator)
            return result
        return await self.context.add_message(message)
