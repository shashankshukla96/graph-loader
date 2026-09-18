#!/usr/bin/env python3
"""Download IMDb's public non-commercial TSV datasets without modifying them."""
from __future__ import annotations

import argparse
from pathlib import Path
import shutil
from urllib.request import Request, urlopen


DATASET_BASE_URL = "https://datasets.imdbws.com"
DATASET_FILES = (
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
)


def download_file(filename: str, output_dir: Path, *, force: bool) -> Path:
    """Stream one official IMDb archive to disk atomically."""
    destination = output_dir / filename
    if destination.exists() and not force:
        print(f"Keeping existing {destination}")
        return destination

    temporary = destination.with_suffix(destination.suffix + ".part")
    request = Request(f"{DATASET_BASE_URL}/{filename}", headers={"User-Agent": "graph-loader-imdb-example/1.0"})
    print(f"Downloading {filename} ...")
    with urlopen(request, timeout=60) as response, temporary.open("wb") as target:
        shutil.copyfileobj(response, target, length=1024 * 1024)
    temporary.replace(destination)
    print(f"Saved {destination}")
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("examples/imdb/data/raw"))
    parser.add_argument("--force", action="store_true", help="replace already-downloaded archives")
    parser.add_argument("--files", nargs="*", choices=DATASET_FILES, default=list(DATASET_FILES))
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for filename in args.files:
        download_file(filename, args.output_dir, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
