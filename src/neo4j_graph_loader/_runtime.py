"""Private CLI attempt paths. Persistent state belongs to the caller, never the install."""
from dataclasses import dataclass
import os
import logging
from pathlib import Path
import stat
import tempfile


@dataclass(frozen=True)
class RunPaths:
    root: Path
    schema: Path
    rejections: Path
    plans: Path
    spill: Path
    timings: Path


def _writable_directory(path: Path) -> Path:
    path = path.resolve()
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Probe actual access; do not chmod caller directories or touch their files.
    descriptor, probe = tempfile.mkstemp(prefix='.graph-loader-probe-', dir=path)
    os.close(descriptor)
    Path(probe).unlink()
    return path


def _writable_file(path: Path) -> None:
    _writable_directory(path.parent)
    if path.exists() or path.is_symlink():
        if not stat.S_ISREG(path.lstat().st_mode):
            raise ValueError(f'output must be a regular file, not a link or special file: {path}')
        descriptor = os.open(path, os.O_WRONLY)
        os.close(descriptor)


def prepare_run(args, config: str) -> RunPaths:
    """Preflight outputs before ingestion, retaining all evidence even on failure.

    Explicit plan/spill/timing paths take priority. An external edge plan is an
    input: only its sibling ledger output is probed. Existing locks remain the
    planner's responsibility; probes never truncate files or remove locks.
    """
    content = Path(config).read_bytes()
    base = getattr(args, 'run_dir', None)
    if base is not None:
        base = _writable_directory(Path(base))
    root = Path(tempfile.mkdtemp(prefix='graph-loader-', dir=base)).resolve()
    args.runtime_root = root
    logging.getLogger(__name__).info('stage=runtime directory=%s', root)
    # mkdtemp provides exclusive0700 ownership, even for repeated run identities.
    schema = root/'runtime_schema.yaml'
    with schema.open('xb') as output:
        os.chmod(schema, 0o600)
        output.write(content)
    rejections = _writable_directory(root/'rejections')
    plans = _writable_directory(Path(args.plan_dir) if getattr(args, 'plan_dir', None) else root/'plans')
    spill = _writable_directory(Path(args.plan_spill_dir) if getattr(args, 'plan_spill_dir', None) else root/'spill')
    timings = _writable_directory(Path(os.environ['GRAPH_LOADER_TIMING_DIR'])
                                  if os.environ.get('GRAPH_LOADER_TIMING_DIR') else root/'timings')
    for name in ('metrics-cli.prom', 'lease-settings.json', 'lease-server.log', 'planned-summary.json',
                 'planned-ledger.json', 'planned-dispatcher.log'):
        _writable_file(timings/name)
    for existing in timings.iterdir():
        if existing.name.startswith(('metrics-', 'stage-', 'substage-', 'node-', 'edge-', 'lease-', 'planned-')):
            _writable_file(existing)
    edge_plan = getattr(args, 'edge_plan', None)
    from neo4j_graph_loader.orchestrator.planned_edges import ledger_path
    plan_path = Path(edge_plan).resolve() if edge_plan and edge_plan != 'auto' else plans/'edge-plan.json'
    ledger = ledger_path(plan_path)
    _writable_file(ledger)
    _writable_file(ledger.with_name(f'.{ledger.name}.tmp'))
    if not edge_plan or edge_plan == 'auto':
        for suffix in ('', '.progress', '.failed'):
            _writable_file(plans/('edge-plan.json'+suffix))
        progress = plans/'edge-plan.json.progress'
        _writable_file(progress.with_name(f'.{progress.name}.tmp'))
    args.plan_dir = str(plans)
    args.plan_spill_dir = str(spill)
    return RunPaths(root, schema, rejections, plans, spill, timings)


def timing_directory(args) -> str | None:
    runtime = getattr(args, 'runtime', None)
    return str(runtime.timings) if runtime is not None else os.environ.get('GRAPH_LOADER_TIMING_DIR')


def timing_kwargs(args) -> dict:
    runtime = getattr(args, 'runtime', None)
    return {'timing_dir': str(runtime.timings)} if runtime is not None else {}
