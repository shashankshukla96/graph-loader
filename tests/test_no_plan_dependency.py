"""E19.F1.S2: no product file or test depends on, or points at, the maintainers' ``plan/`` documents.

The check reads every text file of the product set (the set a clean snapshot publishes). ``plan/``, ``soak/``, ``var/`` and the two
internal release-process records are not part of it. Internal pointers are the paths of the planning folders and files; product
identifiers such as ``graph-loader-edge-plan/1`` and prose such as "plan/chunk" are not pointers.
"""
from pathlib import Path
import re

import pytest

ROOT = Path(__file__).resolve().parents[1]
PRODUCT_FOLDERS = ('src', 'scripts', 'tests', 'docs', 'examples', 'config', 'monitoring')
PRODUCT_FILES = ('README.md', 'PACKAGE_README.md', 'CHANGELOG.md', 'SECURITY.md', 'pyproject.toml', 'MANIFEST.in', 'Makefile',
                 'docker-compose.yaml', 'docker-compose.monitoring.yml', 'requirements.txt', 'requirements-dev.txt', 'requirements-build.txt')
# The test inventory stores the source of the tests (their assertions) as data, so it quotes the strings these checks look for.
INTERNAL_RECORDS = {'docs/PYPI_LIBRARY_CHECKLIST.md', 'docs/PYPI_CHECKLIST_REVIEW.md', 'tests/inventory/test-inventory.json'}
# Built from parts so this file does not match its own pattern.
POINTER = re.compile('plan' + r'/(?:v[0-9]|library-implementation|[a-z0-9-]+\.md)')
LEGACY_NAMES = re.compile('run_e04' + '_fault|verify_e03' + r'_artifacts|\]\([^)]*PYPI_(?:LIBRARY_CHECKLIST|CHECKLIST_REVIEW)')


def product_files():
    for name in PRODUCT_FILES:
        if (ROOT / name).is_file():
            yield ROOT / name
    for folder in PRODUCT_FOLDERS:
        for path in sorted((ROOT / folder).rglob('*')):
            if path.is_file() and '__pycache__' not in path.parts and path.suffix not in {'.pyc', '.png', '.gz', '.whl', '.zip'}:
                yield path


def read(path):
    try:
        return path.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        return None


def test_no_product_file_points_at_a_planning_document():
    found = []
    for path in product_files():
        relative = path.relative_to(ROOT).as_posix()
        text = read(path)
        if text is not None and relative not in INTERNAL_RECORDS and relative != Path(__file__).relative_to(ROOT).as_posix():
            found += [f'{relative}:{n}' for n, line in enumerate(text.splitlines(), 1) if POINTER.search(line)]
    assert found == []


def test_no_product_file_names_a_moved_tool_by_its_old_name_or_links_an_internal_record():
    found = []
    for path in product_files():
        relative = path.relative_to(ROOT).as_posix()
        text = read(path)
        if text is None or relative in INTERNAL_RECORDS or relative == Path(__file__).relative_to(ROOT).as_posix():
            continue
        found += [f'{relative}:{n}' for n, line in enumerate(text.splitlines(), 1) if LEGACY_NAMES.search(line)]
    assert found == []


def test_control_the_pointer_pattern_matches_internal_pointers_only():
    for tail in ('v6/slice-4/stories.md', 'library-implementation/epics/e14.md', 'v5-v6-orchestration.md', 'v6/completion_slice-2.md for why'):
        assert POINTER.search('see plan' + '/' + tail), tail
    for fine in ('graph-loader-edge-plan/1', 'stored plan/chunk/DLQ formats', 'plan/spill/timing overrides', 'plan.json', 'the plan'):
        assert not POINTER.search(fine), fine
    assert LEGACY_NAMES.search('[checklist](PYPI_LIBRARY_CHECKLIST.md)') and not LEGACY_NAMES.search('a PYPI_LIBRARY_CHECKLIST.md mention')


@pytest.mark.parametrize('moved', ['scripts/verify_artifacts.py', 'scripts/check_doc_links.py', 'tests/fixtures/run_fault.py'])
def test_the_tools_the_product_uses_live_in_the_product(moved):
    assert (ROOT / moved).is_file()
