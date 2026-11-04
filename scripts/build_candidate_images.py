#!/usr/bin/env python3
"""Build development candidates locally through the path ``graph-loader images build`` uses; never push, pull release tags or publish.

The candidate wheel (built from this checkout, or given with ``--wheel``: pass the frozen one so the images are bound to it) goes into the generated build context with the hashed dependency lock of the build platform;
the dependencies come from a wheelhouse prepared for that platform (``--wheelhouse``, see docs/DISTRIBUTION_DEVELOPMENT.md) or from an index
(``--pip-index-url``, default the public PyPI). The manifest (``images.json``) is written to ``--output``.
"""
from __future__ import annotations

import argparse
import ast
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from neo4j_graph_loader.image_build import builder  # noqa: E402

NAMESPACE = "ghcr.io/neburyx"
DOCKERFILES = builder.DOCKERFILES
PUBLIC_INDEX = 'https://pypi.org/simple/'


def package_version(root=ROOT):
    tree = ast.parse((root / "src/neo4j_graph_loader/_version.py").read_text())
    return next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "__version__" for t in n.targets))


def build_candidate_wheel(root: Path, outdir: Path) -> Path:
    done = subprocess.run([sys.executable, '-m', 'build', '--wheel', '--no-isolation', '--outdir', str(outdir), str(root)], capture_output=True, text=True)
    wheels = list(outdir.glob('*.whl'))
    if done.returncode or len(wheels) != 1:
        raise SystemExit('could not build the candidate wheel (needs `build` and `setuptools`):\n' + (done.stdout + done.stderr)[-800:])
    return wheels[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="New evidence directory (receives images.json and image-overrides.sh)")
    parser.add_argument("--suffix", required=True, help="Unique local candidate suffix (no release tag overwrite)")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--wheelhouse", type=Path, help="wheelhouse of the locked dependencies for the build platform (the candidate wheel is added to a copy)")
    source.add_argument("--pip-index-url", default=PUBLIC_INDEX, help=f"index for the locked dependencies (default {PUBLIC_INDEX})")
    parser.add_argument("--wheel", type=Path, help="build the images from this candidate wheel (the one the lanes will test) instead of building one from the checkout")
    parser.add_argument("--base-image", default=builder.DEFAULT_BASE)
    parser.add_argument("--pip-ca-bundle", type=Path)
    parser.add_argument("--docker-config", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='candidate-') as work:
        work = Path(work)
        (work / 'wheel').mkdir()
        wheel = args.wheel.resolve() if args.wheel else build_candidate_wheel(ROOT, work / 'wheel')
        if wheel.name != f'neo4j_graph_loader-{package_version()}-py3-none-any.whl':
            raise SystemExit(f'--wheel must be neo4j_graph_loader-{package_version()}-py3-none-any.whl (the version of this checkout)')
        wheelhouse = None
        if args.wheelhouse:
            wheelhouse = work / 'wheelhouse'
            shutil.copytree(args.wheelhouse, wheelhouse, ignore=shutil.ignore_patterns('neo4j_graph_loader-*'))
            shutil.copy(wheel, wheelhouse / wheel.name)
        settings = builder.Settings(
            registry=NAMESPACE, version=package_version(), package_wheel=wheel, base_image=args.base_image, wheelhouse=wheelhouse,
            index_url=None if wheelhouse else args.pip_index_url, ca_bundle=args.pip_ca_bundle, docker_config=args.docker_config,
            tag_suffix=args.suffix, output=args.output.resolve(), bind_to_installed=False)
        try:
            manifest = builder.build(settings)
        except builder.BuildError as exc:
            raise SystemExit(f'candidate build failed: {exc}')
    print(f"Built {len(manifest['images'])} local candidates; manifest: {args.output.resolve() / 'images.json'}; overrides: {args.output.resolve() / 'image-overrides.sh'}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
