"""Host extension points. The default needs only Amplifier's coordinator."""


class HostAdapter:
    async def prepare_execution(self, loop, coordinator, providers):
        runtime = coordinator.get_capability("live.runtime") if coordinator else None
        return runtime, providers, None

    def finite_finished(self, scope, coordinator, status, result=""):
        pass

    def prepare_provider(self, provider, coordinator):
        pass

    def content(self, command, coordinator):
        encode = coordinator.get_capability("live.attachments.encode") if coordinator else None
        if encode is None:
            raise ValueError("This host does not support work attachments")
        return encode(command, coordinator)


class AttachmentContext:
    """Preserve prompt hooks while replacing the corresponding user message."""

    def __init__(self, context, prompt, content):
        self.context, self.prompt, self.content = context, prompt, content
        self.pending = True

    def __getattr__(self, name):
        return getattr(self.context, name)

    async def add_message(self, message):
        if self.pending and message.get("role") == "user" and message.get("content") == self.prompt:
            message = {**message, "content": self.content}
            self.pending = False
        return await self.context.add_message(message)
