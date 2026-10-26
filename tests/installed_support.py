"""Location adapters for unchanged checkout and installed regression assertions."""
import hashlib
import json
from pathlib import Path
import subprocess


def application_root() -> Path:
    """Scan the actual imported package, with explicit nonempty payload checks."""
    import neo4j_graph_loader

    root = Path(neo4j_graph_loader.__file__).resolve().parent
    for relative in ('cli.py', 'loader/node_loader.py', 'loader/edge_loader.py',
                     'orchestrator/lease_server.py', 'utils/metrics.py'):
        assert (root/relative).is_file(), 'incomplete application scan: ' + relative
    assert list(root.rglob('*.py')), 'application scan must not be empty'
    return root


def tracked_reference_files(root: Path) -> list[str]:
    """Use the frozen staging receipt, or Git for the independent checkout lane."""
    manifest = root / 'e07-stage-files.json'
    if manifest.is_file():
        files = json.loads(manifest.read_text())
        references = [name for name in files if name.startswith(('config/', 'examples/'))]
        assert references, 'installed reference inventory must not be empty'
        for name in references:
            assert '..' not in Path(name).parts and not any(p.startswith('.env') for p in Path(name).parts), name
            assert hashlib.sha256((root/name).read_bytes()).hexdigest() == files[name], name
        return references
    return subprocess.run(['git', 'ls-files', 'config', 'examples'], cwd=root,
                          capture_output=True, text=True, check=True).stdout.split()
