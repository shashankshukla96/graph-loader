#!/usr/bin/env python3
"""Extract IMDb gzip archives to byte-preserving TSV files."""
from __future__ import annotations

import argparse
import gzip
from pathlib import Path
import shutil


def extract(source: Path, destination: Path, *, force: bool) -> None:
    """Decompress one archive without parsing or changing its TSV content."""
    if destination.exists() and not force:
        print(f"Keeping existing {destination}")
        return
    temporary = destination.with_suffix(destination.suffix + ".part")
    print(f"Extracting {source.name} ...")
    with gzip.open(source, "rb") as compressed, temporary.open("wb") as extracted:
        shutil.copyfileobj(compressed, extracted, length=1024 * 1024)
    temporary.replace(destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("examples/imdb/data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("examples/imdb/data/extracted"))
    parser.add_argument("--force", action="store_true", help="replace already-extracted TSVs")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    archives = sorted(args.input_dir.glob("*.tsv.gz"))
    if not archives:
        parser.error(f"no .tsv.gz archives found in {args.input_dir}")
    for archive in archives:
        extract(archive, args.output_dir / archive.with_suffix("").name, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
