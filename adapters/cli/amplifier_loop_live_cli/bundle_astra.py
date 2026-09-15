from amplifier_module_loop_live.scope import LIVE_OWNER, NATIVE_REQUEST
"""Native Astra transport extension for the pinned, configured OpenAI provider.

Request construction, native ToolSpecs, response parsing, costing and hooks stay
in the upstream driver. Only Responses transport/lineage and pending async calls
are extended here. Other models and utility calls retain the normal driver path.
"""

import asyncio
import contextvars
import copy
import json
import time
import uuid
from collections import Counter
from urllib.parse import urlparse

import websockets
from websockets.protocol import State
from amplifier_core.message_models import ToolCall
from amplifier_core.llm_errors import LLMError
from amplifier_module_provider_openai import OpenAIProvider, _RawResponseObject
from .image_budget import ImageBudgetMixin



def key(message):
    return json.dumps(message, sort_keys=True, default=str)


def add_usage(total, usage):
    for name, value in usage.items():
        if isinstance(value, dict):
            add_usage(total.setdefault(name, {}), value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            total[name] = total.get(name, 0) + value


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
        try:
            return await super().complete(request,**kwargs)
        except ComputerResultError as exc:
            owner=LIVE_OWNER.get()
            if owner:
                await owner.runtime.emit("provider.error",error_type="computer_capture_failed",public_message=exc.public_message)
            raise


class BundleAstraProvider(ImageBudgetMixin, OpenAIProvider):
    native_bundle_live = True

    @classmethod
    def wrap(cls, original):
        provider = cls.__new__(cls)
        # A session-local extension preserves the mounted driver's credentials,
        # configuration, native serializers, cost contributor and lazy client.
        provider.__dict__ = original.__dict__.copy()
        provider.socket = None
        provider.epoch = str(uuid.uuid4())
        provider.previous_response_id = None
        provider.response_id = None
        provider.owner = None
        provider.inflight = None
        provider.awaiting_successor = False
        provider.seen = Counter()
        provider.last_context = Counter()
        provider.async_calls = {}
        provider.delivered_results = set()
        provider.pending_parent = False
        provider.full_messages = []
        provider.pending_steer = None
        provider.native_calls = {}
        provider.reset_socket = False
        provider.socket_opened_at = None
        provider.request_uncertain = False
        return provider

    async def complete(self, request, **kwargs):
        owner = LIVE_OWNER.get()
        if owner is None or (request.metadata or {}).get("stream") is False or kwargs.get("extended_thinking") is False or kwargs.get("model", self.default_model) != "gpt-6-astra":
            token = NATIVE_REQUEST.set(None)
            try:
                return await super().complete(request, **kwargs)
            finally:
                NATIVE_REQUEST.reset(token)
        if self.owner is not None:
            raise RuntimeError("Native provider already owns a generation")
        self.owner = owner
        token = NATIVE_REQUEST.set(self)
        request = request.model_copy(update={"metadata": {**(request.metadata or {}), "stream": False}})
        try:
            await self._renew_idle_connection()
            # context-simple compacts the request view, not the saved history.
            # Keep original call identities/results outside that lossy view.
            getter = getattr(owner.context, "get_messages", None)
            self.full_messages = await getter() if getter else []
            for message in [*self.full_messages, *(m.model_dump() for m in request.messages)]:
                for call in (message.get("metadata") or {}).get("converge_native_async_calls", []):
                    self.async_calls.setdefault(call["call_id"], call)
            response = await super().complete(request, **kwargs)
            calls = [self.async_calls[call.id] for call in response.tool_calls or [] if call.id in self.async_calls]
            response.metadata = {**(response.metadata or {}), "converge_live_epoch": self.epoch,
                                 "converge_native_async_calls": calls}
            return response
        except Exception as exc:
            from amplifier_loop_live_cli.computer_results import ComputerResultError
            if isinstance(exc,ComputerResultError):
                await owner.runtime.emit("provider.error",error_type="computer_capture_failed",public_message=exc.public_message)
            raise
        finally:
            NATIVE_REQUEST.reset(token)
            self.owner = None
            self.response_id = None
            self.full_messages = []

    def _find_missing_tool_results(self, messages):
        missing = super()._find_missing_tool_results(messages)
        if NATIVE_REQUEST.get() is self:
            # Native async calls are intentionally unpaired while running.
            return [entry for entry in missing if entry[1] not in self.async_calls]
        return missing

    def _convert_messages(self, messages, **kwargs):
        from amplifier_loop_live_cli.computer_results import failed_computer_history,validate_computer_outputs
        messages,_=failed_computer_history(messages)
        if NATIVE_REQUEST.get() is not self:
            return validate_computer_outputs(super()._convert_messages(messages, **kwargs))
        for message in messages:
            for call in (message.get("metadata") or {}).get("converge_native_async_calls", []):
                self.async_calls.setdefault(call["call_id"], call)
        # Compare the normalized request view. The presence of an old failed or
        # superseded screenshot alone must not reset every subsequent request
        # (or let a utility call reset an active manager's lineage).
        persistent = Counter(key(m) for m in messages if m.get("role") != "tool" and not (m.get("metadata") or {}).get("ephemeral"))
        carry = []
        if self.last_context - persistent:
            # A rewritten/compacted window must become a new lineage, so dropped
            # instructions do not linger on the server. Do not discard pending
            # tool identities or accepted steering as a side effect of compaction.
            if self.pending_parent:
                # Only explicit pending evidence permits carrying accepted
                # direction to a fresh connection. Unknown disconnects still
                # fail without replay. Reuse actual results; never rerun tools.
                pending = self.pending_steer
                full = super()._convert_messages(self.full_messages, **kwargs)
                for required in pending["required_input"]:
                    identity = required.get("call_id")
                    original = self.native_calls.get(identity)
                    job = self.owner.native_job(identity)
                    actual = next((i for i in full if i.get("call_id") == identity
                                   and i.get("type") == required["type"]), None)
                    if (not original or not actual or (job is not None and "result" not in job) or not any(m.get("role") == "tool" and
                            m.get("tool_call_id") == identity for m in self.full_messages)):
                        raise LLMError("Pending steering requires a saved tool result before compaction", provider=self.name, retryable=False)
                    carry.extend([copy.deepcopy(original), actual])
                command = pending["command"]
                if not any((m.get("metadata") or {}).get("live_input_id") == command.id for m in messages):
                    carry.append({"role": "user", "content": self.owner._text(command)})
                self.reset_socket = True
            self.previous_response_id = None
            self.seen.clear()
            self.epoch = str(uuid.uuid4())
        self.last_context = persistent
        selected, occurrences = [], Counter()
        for message in messages:
            metadata = message.get("metadata") or {}
            if self.previous_response_id and metadata.get("converge_live_epoch") == self.epoch:
                continue
            call_id = message.get("tool_call_id")
            if message.get("role") == "tool" and call_id in self.async_calls:
                continue  # Reconcile from the original job/full history below.
            identity = key(message)
            occurrences[identity] += 1
            if not self.previous_response_id or metadata.get("ephemeral") or occurrences[identity] > self.seen[identity]:
                selected.append(message)
        self.seen |= occurrences
        # Run the full serializer once to retain native call/namespace maps even
        # when their assistant messages are already in the server's lineage.
        super()._convert_messages(messages, **kwargs)
        items = super()._convert_messages(selected, **kwargs)
        outstanding = {identity for identity in self.async_calls if (job := self.owner.native_job(identity)) is not None
                       and (not job.get("restored") or job.get("recovered")) and identity not in self.delivered_results}
        results = {m.get("tool_call_id"): m.get("content") for m in [*messages, *self.full_messages]
                   if m.get("role") == "tool"}

        def append_call(target, identity):
            if not self.previous_response_id:
                target.append(copy.deepcopy(self.async_calls[identity]))
            elif identity in self.delivered_results:
                return
            job = self.owner.native_job(identity)
            result = job.get("result") if job is not None else results.get(identity)
            if result is not None:
                target.append({"type": "function_call_output", "call_id": identity, "output": result})
                self.delivered_results.add(identity)

        # Restore the native call and its actual result IN PLACE. Moving old
        # calls behind the newest user message makes historical delegations look
        # newly issued. Discard compacted receipts and synchronous repair's
        # fabricated outputs; an unfinished async call stays unpaired.
        reconciled, visible = [], set()
        for item in items:
            identity = item.get("call_id")
            if identity in self.async_calls and item.get("type") in {"function_call", "function_call_output"}:
                if item["type"] == "function_call" and identity not in visible:
                    append_call(reconciled, identity)
                    visible.add(identity)
            else:
                reconciled.append(item)
        # Jobs omitted by compaction still need their original identities on a
        # fresh lineage, before the current direction. A continuation sends only
        # newly available real results after its new input.
        pending_history = []
        for identity in self.async_calls:
            if identity in outstanding - visible:
                append_call(reconciled if self.previous_response_id else pending_history, identity)
        carried_ids = {i.get("call_id") for i in carry if i.get("call_id")}
        return validate_computer_outputs([i for i in pending_history + reconciled if i.get("call_id") not in carried_ids] + carry)

    async def _connect(self):
        if self.socket is not None:
            return
        client = self.client
        base = str(client.base_url).rstrip("/")
        parsed = urlparse(base)
        if parsed.scheme != "https" or parsed.hostname != "api.openai.com":
            raise RuntimeError("Native Astra requires the configured official OpenAI endpoint")
        headers = {name: value for name, value in {**client.auth_headers, **client.default_headers}.items()
                   if isinstance(value, str)}
        headers.pop("Content-Type", None)
        self.socket = await websockets.connect("wss://" + parsed.netloc + parsed.path + "/responses",
            additional_headers=headers, open_timeout=20, close_timeout=5, max_size=16 * 1024 * 1024)
        self.socket_opened_at = time.monotonic()

    async def _renew_idle_connection(self):
        # Called before serialization of a NEW request. Never retry a send/recv:
        # if the previous generation ended ambiguously, its tools may have run.
        if self.request_uncertain:
            raise LLMError("Native response outcome is uncertain; no automatic replay", provider=self.name, retryable=False)
        if self.socket is None:
            return
        closed = getattr(self.socket, "state", State.OPEN) != State.OPEN
        aged = self.socket_opened_at is not None and time.monotonic() - self.socket_opened_at >= 55 * 60
        if not closed and not aged:
            return
        if self.pending_parent or self.inflight or self.awaiting_successor:
            if closed:
                raise LLMError("Native connection closed with pending steering; no automatic replay", provider=self.name, retryable=False)
            return  # Pending successors belong to this socket until resolved.
        await self.socket.close()
        self.socket = None
        self.socket_opened_at = None
        # The configured driver uses store=false. The socket's response cache
        # cannot be resumed on another socket. Serialize the current full window
        # and real outstanding call identities/results, as after compaction.
        self.previous_response_id = None
        self.seen.clear()
        self.epoch = str(uuid.uuid4())
        await self.owner.runtime.emit("native.connection.renewed", reason="closed" if closed else "age",
                                      context="current_window", execution_replayed=False)

    async def steer_live(self, command):
        if not self.owner or not self.response_id or self.inflight or self.awaiting_successor:
            return False
        self.inflight = {"command": command, "accepted": False, "parent": self.response_id}
        await self.socket.send(json.dumps({"type": "response.steer",
            "previous_response_id": self.response_id, "input": self.owner._text(command)}))
        await self.owner.runtime.emit("steering.sent", input_id=command.id, response_id=self.response_id)
        return True

    async def _create_response(self, params):
        if NATIVE_REQUEST.get() is not self:
            return await super()._create_response(params)
        try:
            return await self._native_response(params)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The conventional driver's generic exception handler retries.
            # Native calls may already have dispatched jobs, so explicitly use
            # the kernel's non-retryable contract even for transport failures.
            raise LLMError("Native Responses stopped; no automatic replay: " + str(exc),
                           provider=self.name, retryable=False) from exc

    async def _native_response(self, params):
        if self.reset_socket:
            # Queued steering is connection-local. Close its completed, pending
            # parent before replaying that direction once in the fresh context.
            await self.socket.close()
            self.socket = None
            self.reset_socket = False
        await self._connect()
        payload = copy.deepcopy(params)
        if payload.get("background") or payload.get("stream"):
            raise RuntimeError("Native bundle transport does not support background polling or SSE parameters")
        payload.pop("stream", None)
        # SDK transport-only options are not Responses wire fields.
        for field in ("timeout", "extra_headers", "extra_query"):
            if field in payload:
                raise RuntimeError("Native transport cannot preserve this per-request transport override")
        extra_body = payload.pop("extra_body", {})
        payload.update(extra_body)
        if self.previous_response_id:
            payload["previous_response_id"] = self.previous_response_id
        async_names = set()
        def mark(tools):
            for tool in tools:
                if tool.get("type") == "namespace":
                    mark(tool.get("tools", []))
                elif self.owner.config.get("background_delegate") and tool.get("type") == "function" and tool.get("name") == "delegate":
                    tool["async"] = True
                    async_names.add("delegate")
        mark(payload.get("tools", []))
        from amplifier_loop_live_cli.diagnostics import trace, trace_native_context
        trace_native_context(payload)
        trace("native.send", {"type":"response.create", **payload})
        self.request_uncertain = True
        await self.socket.send(json.dumps({"type": "response.create", **payload}))
        await self.owner.runtime.emit("native.request", continuation=bool(self.previous_response_id),
            input_items=len(payload.get("input", [])), async_tools=sorted(async_names))
        output, output_ids, totals, terminal = [], set(), {}, None
        self.pending_parent = False
        self.pending_steer = None
        async with asyncio.timeout(max(1, self.timeout - 2)):
            while True:
                event = json.loads(await self.socket.recv())
                trace("native.receive", event)
                kind = event.get("type")
                if kind == "response.created":
                    terminal = None
                    self.response_id = event["response"]["id"]
                    if self.awaiting_successor:
                        self.awaiting_successor = False
                        self.inflight = None
                    await self.owner.runtime.emit("native.response.created", response_id=self.response_id)
                elif kind == "response.output_item.done":
                    item = event["item"]
                    if item.get("type") == "function_call" and item.get("async"):
                        if item.get("name") not in async_names:
                            raise RuntimeError("Unexpected native async tool")
                        call_id = item["call_id"]
                        if call_id not in self.async_calls:
                            self.async_calls[call_id] = item
                            await self.owner.start_native_job(ToolCall(id=call_id, name=item["name"],
                                arguments=json.loads(item["arguments"])))
                elif kind == "response.steer.accepted":
                    if not self.inflight:
                        raise RuntimeError("Unmatched steering acknowledgment")
                    self.inflight["accepted"] = True
                    self.awaiting_successor = True
                    command = self.inflight["command"]
                    # Record accepted direction once. The server owns delivery;
                    # the next ordinary request must not submit it a second time.
                    await self.owner.context.add_message({"role": "user", "content": self.owner._text(command),
                        "metadata": {"converge_live_epoch": self.epoch, "live_input_id": command.id}})
                    await self.owner.runtime.emit("steering.accepted", input_id=command.id,
                        response_id=self.inflight["parent"], steer_id=event.get("steer", {}).get("id"))
                elif kind == "response.steer.failed":
                    if not self.inflight:
                        raise RuntimeError("Unmatched steering failure")
                    command = self.inflight["command"]
                    await self.owner.runtime.emit("steering.failed", input_id=command.id)
                    # The API explicitly promises failed input will not be
                    # applied later. Keep it in the request-boundary queue.
                    self.owner.pending.append(command)
                    self.owner.steer("[loop-live inbox wake]")
                    self.inflight, self.awaiting_successor = None, False
                    if terminal:
                        break
                elif kind == "response.steer.pending":
                    if not self.inflight or not self.inflight["accepted"]:
                        raise RuntimeError("Unmatched pending steering")
                    self.awaiting_successor = False
                    self.pending_parent = True
                    self.pending_steer = {"command": self.inflight["command"],
                                          "required_input": event.get("required_input", [])}
                    await self.owner.runtime.emit("steering.pending", input_id=self.inflight["command"].id,
                        response_id=self.previous_response_id)
                    self.inflight = None
                    if terminal:
                        break
                elif kind in {"response.completed", "response.incomplete"}:
                    terminal = event["response"]
                    self.previous_response_id = terminal["id"]
                    self.response_id = None
                    if self.owner.config.get("latest_response_only"):
                        output.clear();output_ids.clear()
                    for item in terminal.get("output", []):
                        if item.get("call_id"):
                            self.native_calls[item["call_id"]] = item
                        identity = item.get("id") or key(item)
                        if identity not in output_ids:
                            output_ids.add(identity)
                            output.append(item)
                    usage = terminal.get("usage") or {}
                    add_usage(totals, usage)
                    if kind == "response.incomplete" and (terminal.get("incomplete_details") or {}).get("reason") != "steered":
                        raise RuntimeError("Native response incomplete: " + str((terminal.get("incomplete_details") or {}).get("reason")))
                    if not self.inflight and not self.awaiting_successor:
                        break
                elif kind in {"response.failed", "error"}:
                    # Deliberately non-retryable: a job may already have run.
                    code = (event.get("error") or (event.get("response") or {}).get("error") or {}).get("code", kind)
                    await self.owner.runtime.emit("native.error", code=code)
                    raise RuntimeError("Native Responses failure; no automatic execution replay: " + str(code))
        terminal = {**terminal, "output": output, "usage": totals}
        self.request_uncertain = False
        return _RawResponseObject(terminal)

    async def close_live(self):
        if self.socket:
            await self.socket.close()
            self.socket = None
        await super().close()


async def install(coordinator, native=True):
    from amplifier_loop_live_cli.consultation_images import ImageChatGPTProvider
    from amplifier_module_provider_openai_chatgpt import ChatGPTProvider
    installed = []
    for name, provider in (coordinator.get("providers") or {}).items():
        if type(provider) is OpenAIProvider:
            wrapped = (BundleAstraProvider if native and provider.default_model == "gpt-6-astra" else SafeOpenAIProvider).wrap(provider)
            await coordinator.mount("providers", wrapped, name=name)
            if isinstance(wrapped,BundleAstraProvider):installed.append(name)
        elif type(provider) is ChatGPTProvider:
            await coordinator.mount("providers", ImageChatGPTProvider.wrap(provider), name=name)
    return installed
