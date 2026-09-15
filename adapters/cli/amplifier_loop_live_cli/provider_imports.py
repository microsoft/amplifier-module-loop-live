"""Keep configured provider packages coherent in an isolated CLI worker.

The pinned core validator executes a package without registering its parent in
sys.modules. Relative imports can then come from the installed fallback revision.
Register the configured OpenAI package first, before any provider is mounted.
"""
import importlib.util
from pathlib import Path
import sys


def import_package(directory, name):
    init = Path(directory) / name / '__init__.py'
    if not init.is_file():
        raise RuntimeError(f'Configured provider package {name} is missing.')
    existing = sys.modules.get(name)
    if existing is not None:
        if Path(existing.__file__).resolve() != init.resolve():
            raise RuntimeError(f'{name} was already imported from another source. Start a fresh isolated worker.')
        return existing
    spec = importlib.util.spec_from_file_location(name, init)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        for key in tuple(sys.modules):
            if key == name or key.startswith(name + '.'):
                del sys.modules[key]
        raise
    return module


async def prepare_openai_import(prepared, plan):
    provider = next((p for p in plan.get('providers', []) if p.get('module') == 'provider-openai'), None)
    if provider is None:
        return
    source = await prepared.resolver.async_resolve('provider-openai', source_hint=provider.get('source'),
        profile_hint=provider.get('source'))
    import_package(source.resolve(), 'amplifier_module_provider_openai')
