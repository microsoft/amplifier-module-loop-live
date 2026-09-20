"""Maintained ecosystem declarations must stay eligible for host updates."""
from pathlib import Path
import re


def test_bundle_and_module_sources_follow_branches():
    root = Path(__file__).resolve().parents[1]
    fixed = re.compile(r'@[a-f0-9]{7,40}(?:[\s"\'#]|$)')
    for path in root.rglob('*'):
        if path.suffix not in {'.toml', '.yaml', '.md'} or any(
                part in {'.venv', '.git', 'tests'} for part in path.relative_to(root).parts):
            continue
        assert not fixed.search(path.read_text()), str(path)
