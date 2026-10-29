"""Bounded ownership controller for new E08 test-only Docker containers."""
from __future__ import annotations

import json
from pathlib import Path
import re
import sys

if __package__:
    from . import verify_installed as supervisor
else:
    import verify_installed as supervisor


def run_container(docker, create_arguments, *, output, label_key, label_value, image, timeout):
    """Always retain receipts; remove only a fresh CID with its matching owner label.

    The caller supplies an immutable image ID and finite nonsecret create args.
    No name-based cleanup, prune, global environment change or foreign deletion.
    """
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    receipts = []
    cidfile = output/'container.cid'
    identity = None
    cid = None
    failure = None
    cleanup_errors = []
    cleanup = {}

    def invoke(arguments, name, *, codes=(0,), limit=30):
        stdout = output/(name+'.log')
        stderr = output/(name+'.stderr.log')
        supervisor.run([docker, *arguments], cwd=output, env=supervisor.runtime_environment(Path(docker)),
                       log=stdout, stderr_log=stderr, timeout=limit, receipts=receipts, allowed_exit_codes=codes)
        return receipts[-1]['exit_code'], stdout.read_text(), stderr.read_text()

    def inspect(name):
        code, raw, error = invoke(['inspect', cid], name, codes=(0, 1), limit=20)
        if code:
            if 'no such object' in error.lower():
                return None
            raise RuntimeError('container ownership inspection unavailable: '+error)
        rows = json.loads(raw)
        row = rows[0]
        if (len(rows) != 1 or row['Id'] != cid or row['Image'] != image
                or row['Config']['Labels'].get(label_key) != label_value
                or (identity is not None and row['Created'] != identity['Created'])):
            raise RuntimeError('container ownership identity differs; refusing cleanup')
        return row

    try:
        # A new cidfile binds interrupted docker-create to this exact launch.
        invoke(['create', '--cidfile', str(cidfile), '--label', f'{label_key}={label_value}',
                *create_arguments], 'create')
        cid = cidfile.read_text().strip()
        if re.fullmatch('[a-f0-9]{64}', cid) is None:
            raise RuntimeError('create did not supply a complete owned container ID')
        identity = inspect('initial-inspect')
        if identity is None:
            raise RuntimeError('new container disappeared before execution')
        invoke(['start', '--attach', cid], 'start', limit=timeout)
        final = inspect('final-inspect')
        if final is None or final['State']['Running'] or final['State']['ExitCode'] != 0:
            raise RuntimeError('container did not complete successfully')
    except BaseException as exc:
        failure = exc
    finally:
        # Recover a CID even when create or initial inspection was interrupted.
        # Fresh matching label/image inspection is mandatory before every rm.
        if cid is None and cidfile.exists():
            candidate = cidfile.read_text().strip()
            if re.fullmatch('[a-f0-9]{64}', candidate):
                cid = candidate
            else:
                cleanup_errors.append('incomplete create CID; ownership cannot be established')
        if cid is not None:
            for attempt in range(2):
                try:
                    current = inspect(f'cleanup-inspect-{attempt}')
                    cleanup['identity'] = current
                    if current is not None:
                        invoke(['rm', '--force', cid], f'remove-{attempt}', limit=20)
                        if inspect(f'after-remove-{attempt}') is not None:
                            raise RuntimeError('owned container remains after removal')
                    cleanup['absent'] = True
                    break
                except BaseException as exc:
                    cleanup_errors.append(type(exc).__name__+': '+str(exc))
                    if not isinstance(exc, Exception) and failure is None:
                        failure = exc
        else:
            # No CID from create means no ownership claim. A failed create is
            # still a failure; absence of a CID cannot convert it into a pass.
            cleanup['cid_supplied'] = False
        result = {'container_id':cid, 'initial_identity':identity, 'cleanup':cleanup,
                  'cleanup_errors':cleanup_errors, 'commands':receipts,
                  'status':'failed' if failure is not None or cleanup_errors else 'observed'}
        if failure is not None:
            result.update(error=str(failure), exception=type(failure).__name__)
        (output/'controller.json').write_text(json.dumps(result, indent=2)+'\n')
    if failure is not None:
        raise failure
    if cleanup_errors:
        raise RuntimeError('owned container cleanup encountered errors; retained controller.json')
    return result


if __name__ == '__main__':
    sys.exit('Import run_container from an explicit matrix controller; no ambient Docker action is defined.')
