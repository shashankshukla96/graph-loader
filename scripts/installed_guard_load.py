"""Load the installed-runner guard even when a distribution sitecustomize shadows it.

Debian/Ubuntu Python ships /usr/lib/python3.12/sitecustomize.py, which precedes
site-packages on sys.path, so a guard installed as site-packages/sitecustomize.py
would silently never run. A .pth line imports this module during site setup.
"""
import importlib.util
import sys
import sysconfig
from pathlib import Path

_guard = Path(sysconfig.get_paths()['purelib']) / 'sitecustomize.py'
if 'sitecustomize' not in sys.modules and _guard.is_file():
    _spec = importlib.util.spec_from_file_location('sitecustomize', _guard)
    _module = importlib.util.module_from_spec(_spec)
    sys.modules['sitecustomize'] = _module
    _spec.loader.exec_module(_module)
