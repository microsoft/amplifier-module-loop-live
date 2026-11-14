"""Amplifier CLI session setup; the orchestrator owns no CLI imports."""

from contextlib import contextmanager

from amplifier_module_loop_live.host import HostAdapter


class CLIHostAdapter(HostAdapter):
    @contextmanager
    def execution_registration(self, coordinator, hooks):
        factory = coordinator.get_capability("cli.media.execution_registration") if coordinator else None
        if factory is None:
            yield coordinator, hooks
            return
        if not callable(factory):
            raise TypeError("CLI execution registration must be a context factory")
        # The CLI owns the actual graph transition and receipt. The adapter
        # only carries its supplied registration view through real setup.
        with factory() as selected:
            selected_hooks = getattr(selected, "hooks", None)
            if (not callable(getattr(selected, "get_capability", None))
                    or not callable(getattr(selected, "mount", None))
                    or not callable(getattr(selected_hooks, "register", None))
                    or not callable(getattr(selected_hooks, "emit", None))):
                raise TypeError("CLI execution registration must yield a coordinator view")
            yield selected, selected_hooks

    async def prepare_execution(self, loop, coordinator, providers):
        if coordinator:
            from amplifier_loop_live_cli.diagnostics import install_hooks
            install_hooks(coordinator)
        # The pinned CLI spawner registers its terminal approval provider after
        # child initialization. Restore this host's shared approval channel
        # before the child can execute a tool.
        from amplifier_loop_live_cli.configured import ManagerApprovals, APPROVAL_CHANNEL
        approvals = APPROVAL_CHANNEL.get() or getattr(coordinator, "approval_system", None)
        register = coordinator.get_capability("approval.register_provider") if coordinator else None
        if coordinator and isinstance(approvals, ManagerApprovals):
            coordinator.approval_system = approvals
        if register and isinstance(approvals, ManagerApprovals):
            register(approvals)
        runtime = coordinator.get_capability("live.runtime") if coordinator else None
        from amplifier_loop_live_cli.children import CHILD_SCOPE
        scope=CHILD_SCOPE.get()
        finite_scope=None
        if runtime is None and scope and coordinator:
            # Keep the original CLI spawner suspended around execute(). It owns
            # overlay construction, transcript checkpoints and final cleanup.
            runtime=scope["registry"].attach(coordinator,scope)
            CHILD_SCOPE.set(None)
            from amplifier_loop_live_cli.bundle_astra import install
            await install(coordinator,native=scope.get("persistent",True))
            if not scope.get("persistent",True):
                finite_scope=scope
                runtime=None
            from amplifier_loop_live_cli.children import install as install_children
            await install_children(coordinator,scope["registry"].root,registry=scope["registry"])
            providers=coordinator.get("providers")
            from amplifier_loop_live_cli.worker_settings import ObservedWorkerProvider,effective_selection
            selected=loop._select_provider(providers)
            try:
                scope["registry"].configuration(coordinator.session_id,effective_selection(selected,providers),
                    status="running",native=bool(runtime and getattr(selected,"native_bundle_live",False)),
                    steering="native" if runtime and getattr(selected,"native_bundle_live",False) else "request_boundary" if runtime else "parent_operation")
            except Exception:
                scope["registry"].finite_finished(coordinator.session_id,"error")
                raise
            loop.root_provider=ObservedWorkerProvider(selected,providers,scope["registry"],coordinator.session_id)
        return runtime, providers, finite_scope

    def finite_finished(self, scope, coordinator, status, result=""):
        if scope:
            scope["registry"].finite_finished(coordinator.session_id, status, result)

    def prepare_provider(self, provider, coordinator):
        from amplifier_loop_live_cli.selection import RootProvider
        from amplifier_loop_live_cli.worker_settings import ObservedWorkerProvider
        # Selection/observation and their diagnostics must see canonical refs,
        # never the private hydrated copy. Preserve their type and policy.
        if isinstance(provider, (RootProvider, ObservedWorkerProvider)):
            attribute = "provider" if isinstance(provider, RootProvider) else "original"
            original = getattr(provider, attribute)
            alias = (provider.selection["instance"] if isinstance(provider, RootProvider)
                     else provider.alias or next((name for name, item in provider.providers.items() if item is original), None))
            prepared = self._prepare_provider(original, coordinator, alias=alias)
            if prepared is original:
                return provider
            if isinstance(provider, ObservedWorkerProvider):
                view = ObservedWorkerProvider(prepared, provider.providers, provider.registry, provider.identity)
                view.alias = alias
            else:
                view = RootProvider(prepared, provider.selection, provider.family, provider.runtime)
            view.__wrapped__ = getattr(provider, "__wrapped__", original)
            return view
        providers = coordinator.get("providers") or {} if coordinator else {}
        alias = next((name for name, item in providers.items() if item is provider), None)
        return self._prepare_provider(provider, coordinator, alias=alias)

    def _prepare_provider(self, provider, coordinator, *, alias):
        if getattr(provider, "_loop_live_cli_media_owner", None) is coordinator and coordinator is not None:
            return provider
        from amplifier_loop_live_cli.computer_results import prepare_computer_provider
        prepare_computer_provider(provider, coordinator)
        factory = coordinator.get_capability("cli.media.prepare_provider") if coordinator else None
        if not callable(factory):
            return provider
        prepared = factory(provider, alias=alias)
        if not callable(getattr(prepared, "complete", None)):
            raise TypeError("CLI media factory must return a provider")
        prepared._loop_live_cli_media_owner = coordinator
        prepared.__wrapped__ = provider
        # Foundation intentionally has no catch-all __getattr__. These are live
        # controls, not request-consuming optional provider capabilities.
        for name in ("native_bundle_live", "steer_live", "close_live", "priority"):
            if hasattr(provider, name):
                setattr(prepared, name, getattr(provider, name))
        return prepared

    def content(self, command, coordinator):
        from amplifier_loop_live_cli.work_attachments import work_content
        return work_content(command, coordinator)
