"""Amplifier CLI session setup; the orchestrator owns no CLI imports."""

from amplifier_module_loop_live.host import HostAdapter


class CLIHostAdapter(HostAdapter):
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
            from amplifier_loop_live_cli.computer_results import prepare_computer_provider
            prepare_computer_provider(selected,coordinator)
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
        from amplifier_loop_live_cli.computer_results import prepare_computer_provider
        prepare_computer_provider(provider, coordinator)

    def content(self, command, coordinator):
        from amplifier_loop_live_cli.work_attachments import work_content
        return work_content(command, coordinator)
