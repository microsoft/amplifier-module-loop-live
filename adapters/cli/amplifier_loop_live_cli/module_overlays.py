"""Auditable application-local fixes without editing cached upstream modules."""
import hashlib
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

OLD='''        full_image = Quartz.CGDisplayCreateImage(display_id)
        if full_image is None:
            raise BackendError(self._capture_none_error(display_id))'''
NEW='''        full_image = Quartz.CGDisplayCreateImage(display_id)
        if full_image is None:
            from .converge_macos_capture import capture_single_display
            full_image = capture_single_display(Quartz, display_id, ids, _macos_session_state, BackendError)'''
KEY_OLD='        lowered = part.lower()'
KEY_NEW='''        lowered = part.lower()
        lowered = {"arrowdown":"down", "arrowup":"up", "arrowleft":"left", "arrowright":"right"}.get(lowered,lowered)'''


def computer_overlay(source, cache=None):
    source=Path(source)
    package=source/"amplifier_module_tool_computer_use"
    path=package/"macos.py"
    if not path.is_file():return source,None
    original=path.read_text()
    transport_path=package/"ssh_transport.py"
    transport=transport_path.read_text() if transport_path.exists() else ""
    payload_anchor='    "macos.py",'
    if original.count(OLD)!=1 or original.count(KEY_OLD)!=1 or transport.count(payload_anchor)!=1:
        logging.getLogger(__name__).warning("Computer capture source differs from the reviewed integration; leaving the upstream module unchanged")
        return source,None
    helper=Path(__file__).with_name("macos_capture.py").read_text()
    digest=hashlib.sha256((str(source)+helper+NEW+KEY_NEW).encode())
    for item in sorted(source.rglob("*")):
        if item.is_file() and not any(part in {".git","__pycache__",".venv",".pytest_cache","tests"} for part in item.relative_to(source).parts) and item.suffix!=".pyc":
            digest.update(str(item.relative_to(source)).encode());digest.update(item.read_bytes())
    identity=digest.hexdigest()[:24]
    root=Path(cache) if cache else Path.home()/".amplifier/converge-modules"
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    destination=root/("computer-"+identity)
    manifest={"extension":"macos-capture-and-arrow-keys-v2","upstreamPath":str(source),"sourceSHA256":hashlib.sha256(original.encode()).hexdigest(),"overlayId":identity}
    if not destination.exists():
        temporary=Path(tempfile.mkdtemp(prefix=".computer-",dir=root))
        try:
            shutil.copytree(source,temporary,dirs_exist_ok=True,ignore=shutil.ignore_patterns("__pycache__","*.pyc","tests",".pytest_cache",".venv"))
            (temporary/"amplifier_module_tool_computer_use/macos.py").write_text(original.replace(OLD,NEW).replace(KEY_OLD,KEY_NEW))
            (temporary/"amplifier_module_tool_computer_use/converge_macos_capture.py").write_text(helper)
            (temporary/"amplifier_module_tool_computer_use/ssh_transport.py").write_text(transport.replace(payload_anchor,payload_anchor+'\n    "converge_macos_capture.py",'))
            (temporary/"converge-overlay.json").write_text(json.dumps(manifest,indent=2)+"\n")
            try:os.rename(temporary,destination)
            except OSError:
                if not destination.exists():raise
        finally:
            if temporary.exists():shutil.rmtree(temporary)
    return destination,manifest
