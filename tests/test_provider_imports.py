import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

path = Path(__file__).resolve().parents[1] / 'adapters/cli/amplifier_loop_live_cli/provider_imports.py'
spec = importlib.util.spec_from_file_location('provider_imports_under_test', path)
imports = importlib.util.module_from_spec(spec)
spec.loader.exec_module(imports)


class ProviderImportsTests(unittest.TestCase):
    def test_configured_parent_and_helpers_share_the_same_source(self):
        name = 'configured_provider_import_fixture'
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for revision, helper in [('installed', 'old = True'), ('configured', 'new = True')]:
                package = root / revision / name
                package.mkdir(parents=True)
                (package / '__init__.py').write_text('from .helper import new' if revision == 'configured' else 'from .helper import old')
                (package / 'helper.py').write_text(helper)
            sys.path.insert(0, str(root / 'installed'))
            try:
                inspected = imports.import_package(root / 'installed', name)
                self.assertTrue(inspected.old)
                module = imports.import_package(root / 'configured', name, replace_inspected=True)
                self.assertTrue(module.new)
                self.assertEqual(Path(sys.modules[name + '.helper'].__file__).parent, root / 'configured' / name)
                self.assertIs(imports.import_package(root / 'configured', name), module)
                with self.assertRaisesRegex(RuntimeError, 'another source'):
                    imports.import_package(root / 'installed', name)
                self.assertIs(sys.modules[name], module)
                (root / 'installed' / name / '__init__.py').write_text('from . import helper\nraise ValueError("broken")')
                with self.assertRaisesRegex(ValueError, 'broken'):
                    imports.import_package(root / 'installed', name, replace_inspected=True)
                self.assertIs(sys.modules[name], module)
                self.assertTrue(sys.modules[name + '.helper'].new)
            finally:
                sys.path.pop(0)
                for key in tuple(sys.modules):
                    if key == name or key.startswith(name + '.'):
                        del sys.modules[key]

    def test_failed_import_does_not_leave_a_partial_package(self):
        name = 'failed_provider_import_fixture'
        with tempfile.TemporaryDirectory() as temp:
            package = Path(temp) / name
            package.mkdir()
            (package / '__init__.py').write_text('from . import helper\nraise ValueError("fixture")')
            (package / 'helper.py').write_text('value = True')
            with self.assertRaisesRegex(ValueError, 'fixture'):
                imports.import_package(temp, name)
            self.assertNotIn(name, sys.modules)
            self.assertNotIn(name + '.helper', sys.modules)
