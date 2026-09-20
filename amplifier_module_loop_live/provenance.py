"""Persist input origin independently of provider-facing role and content."""

def input_metadata(command):
    return {'amplifier_input': {'version': 1, 'kind': command.kind,
        'id': command.id, 'source': command.source,
        **({'call_id': command.call_id} if command.call_id else {})}}


class InputContext:
    """Tag exactly the initial prompt row without changing its model semantics."""
    def __init__(self, context, prompt, command):
        self.context, self.prompt, self.command = context, prompt, command
        self.pending = True

    def __getattr__(self, name):
        return getattr(self.context, name)

    async def add_message(self, message):
        if self.pending and message.get('role') == 'user' and message.get('content') == self.prompt:
            message = {**message, 'metadata': {**(message.get('metadata') or {}), **input_metadata(self.command)}}
            self.pending = False
        return await self.context.add_message(message)
