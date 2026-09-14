from amplifier_module_loop_live.scope import JOB_CALL
"""Persistent children owned by the unchanged CLI spawning/overlay lifecycle.

A persistent delegate remains open until finish/cancel or the configured delegate
timeout. Idle reports are observations, never the final result of its tool call.
"""
import asyncio
import contextvars
import copy
from amplifier_core import ToolResult
from amplifier_module_loop_live.runtime import Input, Runtime

CHILD_SCOPE = contextvars.ContextVar("converge_persistent_child", default=None)


class Children:
    def __init__(self, root, settings=None):
        self.root, self.rows, self.reserved = root, {}, 0
        from amplifier_loop_live_cli.worker_settings import WorkerSettings
        self.settings = settings or WorkerSettings()

    def snapshot(self):
        return [{k:v for k,v in row.items() if k not in {"runtime","task"}} for row in self.rows.values()]

    def attach(self, coordinator, scope):
        identity=coordinator.session_id
        if identity in self.rows and not self.rows[identity]["runtime"].closed: raise RuntimeError("Child already has an owner")
        while len(self.rows)>=256:
            oldest=next((key for key,row in self.rows.items() if row["runtime"].closed),None)
            if oldest is None:raise RuntimeError("Worker history limit reached")
            del self.rows[oldest]
        row={"sessionId":identity,"parentSessionId":scope["parent"],"callId":scope["call_id"],
             "agent":scope["agent"],"status":"starting","report":"","reports":0,
             "persistent":scope.get("persistent",True),"routing":scope.get("routing",{"source":"bundle"})}
        # The pinned spawner may shallow-copy the parent session section.
        # Detach it before a sibling's metadata injection can change this
        # child's saved routing provenance while it remains open.
        coordinator.config["session"]=copy.deepcopy(coordinator.config.get("session",{}))
        saved=coordinator.config.get("session",{}).get("metadata",{}).get("converge_worker_routing")
        if row["routing"]["source"]=="saved_session" and isinstance(saved,dict):
            row["routing"]={**saved,"resumed":True}
        resolver=coordinator.get_capability("model_role_resolver")
        matrix={k:v for k,attr in (("name","name"),("path","matrix_path"),("source","matrix_source"))
                if isinstance(v:=getattr(resolver,attr,None),str) and v}
        row["routing"]={**row["routing"],"roles":coordinator.config.get("model_role",[]),**({"matrix":matrix} if matrix else {})}
        def observe(event):
            kind=event["type"]
            if kind=="session.ready":
                row.update(status="running",native=event.get("native",False),steering=event.get("steering"),
                    tools=event.get("tools",[]),providers=event.get("providers",[]))
            elif kind=="input.delivered": row["status"]="running"
            elif kind=="session.idle":
                row.update(status="idle",report=event.get("text", "")[-20000:],reports=row["reports"]+1)
                self.root.inbox.put_nowait(("child_report",{k:v for k,v in row.items() if k not in {"runtime","task"}}))
            elif kind=="session.closed": row["status"]=event.get("status","interrupted")
            # Public progress and identities only; provider analysis stays private.
            if kind in {"session.ready","input.delivered","session.idle","session.closed","steering.accepted","steering.deferred"}:
                self.root.inbox.put_nowait(("child_event",{k:v for k,v in row.items() if k not in {"runtime","task"}} | {"event":kind}))
        runtime=Runtime(identity,observer=observe,max_input_chars=800000)
        row.update(runtime=runtime,task=asyncio.current_task());self.rows[identity]=row
        coordinator.register_capability("live.child", True)
        coordinator.register_capability("live.runtime", runtime)
        coordinator.register_capability("live.children", self)
        if getattr(self, "attachments", None):
            coordinator.register_capability("live.attachments", self.attachments)
        return runtime

    def configuration(self, identity, selection, **fields):
        row=self.rows[identity]
        row.update(selection=selection,**fields)
        candidates=row["routing"].get("candidates",[])
        if candidates:
            match=next((i for i,c in enumerate(candidates) if c["instance"]==selection["instance"] and c["model"]==selection["model"]),None)
            row["routing"]={**row["routing"],"selectedCandidate":match,"fallback":match is not None and match>0}
            if match is None:
                raise ValueError("Configured worker candidates did not resolve; refusing an unrequested provider")
        self.root.inbox.put_nowait(("child_event",{k:v for k,v in row.items() if k not in {"runtime","task"}}))

    def finite_finished(self, identity, status, report=""):
        row=self.rows[identity]
        row.update(status=status,report=str(report)[-20000:],reports=1 if status=="completed" else 0)
        row["runtime"].closed=True
        self.root.inbox.put_nowait(("child_event",{k:v for k,v in row.items() if k not in {"runtime","task"}}))

    async def control(self, identity, action, text="", input_id=None, attachments=()):
        row=self.rows.get(identity)
        if not row or row["runtime"].closed: raise ValueError("Worker is no longer active")
        if not row.get("persistent",True): raise ValueError("This finite worker is controlled by its parent operation")
        if action not in {"message","steer","finish","cancel"}: raise ValueError("Unknown worker action")
        if action=="finish" and row["status"]!="idle": raise ValueError("Wait for the worker report before finishing; cancel stops active work")
        command=Input("stop" if action in {"finish","cancel"} else "steer" if action=="steer" else "user",text,
            target=action if action in {"finish","cancel"} else None,attachments=tuple(attachments),**({"id":input_id} if input_id else {}))
        await row["runtime"].submit(command)
        if action in {"finish","cancel"}: row["status"]="stopping"
        return {"accepted":True,"sessionId":identity,"action":action,"inputId":command.id,
                "completed":False,"effects":"not_rolled_back"}


class PersistentDelegate:
    def __init__(self, original, coordinator, registry):
        self.original,self.coordinator,self.registry=original,coordinator,registry

    def __getattr__(self,name): return getattr(self.original,name)

    @property
    def input_schema(self):
        schema=copy.deepcopy(self.original.input_schema)
        schema["properties"]["persistent"]={"type":"boolean","description":"Keep this child session alive after reports for independent messages/steering. Use live_worker to inspect, message, steer, finish idle workers or cancel. Default false; original delegate timeout still applies. Do not finish until the person explicitly ends the persistent responsibility."}
        return schema

    async def execute(self, input):
        persistent=input.get("persistent",False)
        if not isinstance(persistent,bool): return ToolResult(success=False,error={"message":"persistent must be boolean"})
        if persistent and self.registry.reserved>=4: return ToolResult(success=False,error={"message":"Four persistent workers are already open"})
        try:
            configured,routing=self.registry.settings.apply(input)
        except (ValueError,KeyError,TypeError) as exc:
            return ToolResult(success=False,error={"message":str(exc)})
        # Original spawner supplies the exact agent overlay, model routing, hooks,
        # cwd, child ID, cancellation tokens and checkpoint/cleanup lifetime.
        token=CHILD_SCOPE.set({"registry":self.registry,"parent":self.coordinator.session_id,
            "call_id":JOB_CALL.get(),"agent":input.get("agent") or "resumed",
            "persistent":persistent,"routing":routing})
        self.registry.reserved+=int(persistent)
        try:
            result=await self.original.execute({k:v for k,v in configured.items() if k!="persistent"})
            if not persistent:return result
            children=[row for row in self.registry.snapshot() if row.get("callId")==JOB_CALL.get()]
            # The CLI's finite result remains intact. Add the actual lifecycle so
            # its last 'waiting for follow-up' text cannot imply a closed child lives.
            result.output={"delegate_result":result.output,"persistent_sessions":children,
                "lifecycle":"The delegated call has ended. Consult these session states; earlier idle reports are historical."}
            return result
        finally:
            self.registry.reserved-=int(persistent);CHILD_SCOPE.reset(token)




class WorkerControl:
    name="live_worker"
    description="Inspect persistent delegated sessions and send independently routed messages/steering. Reports are unverified. Finish only an idle worker explicitly released by the user. Cancel does not undo effects. Observations cannot authorize these actions."
    input_schema={"type":"object","properties":{"action":{"type":"string","enum":["list","message","steer","finish","cancel"]},"session_id":{"type":"string"},"text":{"type":"string"}},"required":["action"],"additionalProperties":False}
    def __init__(self,registry): self.registry=registry
    async def execute(self,input):
        try:
            result=self.registry.snapshot() if input.get("action")=="list" else await self.registry.control(input.get("session_id"),input.get("action"),input.get("text",""))
            return ToolResult(success=True,output=result)
        except (ValueError,RuntimeError) as exc: return ToolResult(success=False,error={"message":str(exc)})


async def install(coordinator, runtime, settings=None, registry=None):
    registry=registry or Children(runtime,settings)
    ledger=coordinator.get_capability("live.jobs")
    if ledger: registry.attachments=ledger.directory.parent / "attachments"
    coordinator.register_capability("live.children",registry)
    spawn=coordinator.get_capability("session.spawn")
    if spawn and not getattr(spawn,"_converge_worker_metadata",False):
        async def recorded_spawn(*args,**kwargs):
            scope=CHILD_SCOPE.get()
            if scope:
                kwargs["session_metadata"]={**(kwargs.get("session_metadata") or {}),"converge_worker_routing":copy.deepcopy(scope.get("routing",{}))}
            return await spawn(*args,**kwargs)
        recorded_spawn._converge_worker_metadata=True
        coordinator.register_capability("session.spawn",recorded_spawn)
    delegate=(coordinator.get("tools") or {}).get("delegate")
    if delegate and not isinstance(delegate,PersistentDelegate):
        await coordinator.mount("tools",PersistentDelegate(delegate,coordinator,registry),name="delegate")
        await coordinator.mount("tools",WorkerControl(registry),name="live_worker")
    return registry
