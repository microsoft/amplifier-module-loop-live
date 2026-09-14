"""Bounded single-display fallback for macOS returning no CGDisplayCreateImage.

Copied into the configured computer-use module's local source overlay. The
module still owns target selection, disclosure, presence, lock and input guards.
"""
import subprocess
import tempfile
from pathlib import Path


def capture_single_display(quartz, display_id, display_ids, state, error_type):
    if len(display_ids)!=1 or display_id!=quartz.CGMainDisplayID():
        raise error_type("Core Graphics returned no image; the platform capture fallback requires exactly one active main display")
    if state()[0]!="unlocked":
        raise error_type("Refusing platform screen capture while the console is locked or its state is unknown")
    mode=quartz.CGDisplayCopyDisplayMode(display_id)
    expected=(quartz.CGDisplayModeGetPixelWidth(mode),quartz.CGDisplayModeGetPixelHeight(mode))
    # TemporaryDirectory is private (0700), and the image is removed even on
    # timeout/permission failure. No clipboard, preview window or sound flags.
    with tempfile.TemporaryDirectory(prefix="amplifier-screen-") as directory:
        path=Path(directory)/"screen.png"
        try:
            result=subprocess.run(["/usr/sbin/screencapture","-x","-m","-t","png",str(path)],capture_output=True,timeout=15)
        except subprocess.TimeoutExpired:
            raise error_type("The macOS screenshot utility timed out; no screenshot was returned") from None
        if result.returncode or not path.exists():
            raise error_type("The macOS screenshot utility could not capture the configured display")
        if path.stat().st_size>32*1024*1024:
            raise error_type("The macOS screenshot exceeds the 32 MB capture limit")
        data=path.read_bytes()
    if state()[0]!="unlocked":
        raise error_type("Console lock state changed during capture; screenshot discarded")
    if not data.startswith(b"\x89PNG\r\n\x1a\n"):
        raise error_type("The macOS screenshot utility did not return PNG data")
    source=quartz.CGImageSourceCreateWithData(data,None)
    image=quartz.CGImageSourceCreateImageAtIndex(source,0,None) if source else None
    if image is None or (quartz.CGImageGetWidth(image),quartz.CGImageGetHeight(image))!=expected:
        raise error_type("Platform screenshot dimensions differ from the configured display; refusing uncertain coordinates")
    return image
