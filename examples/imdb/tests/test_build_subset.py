"""Regression test for the local IMDb subset builder using tiny official-shaped TSVs."""
from __future__ import annotations

import csv
import json
from pathlib import Path
import subprocess
import sys

import pytest

from examples.imdb import build_subset


EXAMPLE_DIR = Path(__file__).resolve().parents[1]


def write_tsv(path: Path, fields: list[str], records: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, delimiter="\t", lineterminator="\n")
        writer.writerow(fields)
        writer.writerows(records)


def test_subset_expands_title_person_title_and_writes_graph_files(tmp_path: Path) -> None:
    source, output = tmp_path / "source", tmp_path / "out"
    source.mkdir()
    write_tsv(source / "title.basics.tsv", ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"], [
        ["tt0001", "movie", "Seed", "Seed", "0", "2000", r"\N", "100", "Drama,Comedy"],
        ["tt0002", "movie", "Related", "Related", "0", "2001", r"\N", "90", "Drama"],
        ["tt0003", "movie", "Overflow", "Overflow", "0", "2002", r"\N", "80", "Action"],
    ])
    write_tsv(source / "name.basics.tsv", ["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"], [
        ["nm0001", "Ada", "1970", r"\N", "actor,writer", "tt0001,tt0002"],
        ["nm0002", "Grace", r"\N", r"\N", "director", "tt0001"],
    ])
    write_tsv(source / "title.principals.tsv", ["tconst", "ordering", "nconst", "category", "job", "characters"], [
        ["tt0001", "1", "nm0001", "actor", r"\N", '["Hero"]'],
        ["tt0001", "2", "nm9999", "actor", r"\N", '["Ghost"]'],
        ["tt0002", "1", "nm0001", "writer", r"\N", r"\N"],
        ["tt0003", "1", "nm0002", "director", r"\N", r"\N"],
        ["tt9999", "1", "nm0001", "actor", r"\N", '["Missing title"]'],
    ])
    write_tsv(source / "title.crew.tsv", ["tconst", "directors", "writers"], [
        ["tt0001", "nm0002", "nm0001"], ["tt0002", r"\N", "nm0001"], ["tt0003", "nm0002", r"\N"],
    ])
    write_tsv(source / "title.episode.tsv", ["tconst", "parentTconst", "seasonNumber", "episodeNumber"], [["tt0002", "tt0001", "1", "1"]])
    write_tsv(source / "title.akas.tsv", ["titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"], [["tt0001", "1", "Seed US", "US", "en", r"\N", r"\N", "0"]])
    write_tsv(source / "title.ratings.tsv", ["tconst", "averageRating", "numVotes"], [["tt0001", "7.5", "10"], ["tt0002", "8.0", "5"]])

    subprocess.run([
        sys.executable, str(EXAMPLE_DIR / "build_subset.py"), "--input-dir", str(source),
        "--output-dir", str(output), "--titles", "1", "--depth", "2", "--max-titles", "4", "--max-people", "3",
    ], check=True)

    subset = output / "titles-1-depth-2"
    title_rows = list(csv.DictReader((subset / "graph" / "nodes" / "title.tsv").open(), delimiter="\t"))
    person_rows = list(csv.DictReader((subset / "graph" / "nodes" / "person.tsv").open(), delimiter="\t"))
    assert {row["tconst"] for row in title_rows} == {"tt0001", "tt0002", "tt0003"}
    assert {row["nconst"] for row in person_rows} == {"nm0001", "nm0002"}
    manifest = json.loads((subset / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["selected"]["titles"] == 3
    assert manifest["selected"]["people"] == 2
    assert manifest["seed_offset"] == 0
    assert (subset / "source" / "title.principals.tsv").is_file()
    acted_in = list(csv.DictReader((subset / "graph" / "relationships" / "acted_in.tsv").open(), delimiter="\t"))
    assert [(row["person_id"], row["title_id"]) for row in acted_in] == [("nm0001", "tt0001")]
    credited_on = list(csv.DictReader((subset / "graph" / "relationships" / "credited_on.tsv").open(), delimiter="\t"))
    assert {(row["person_id"], row["title_id"]) for row in credited_on} == {
        ("nm0001", "tt0001"), ("nm0001", "tt0002"), ("nm0002", "tt0003")
    }
    assert "Ghost" not in (subset / "graph" / "nodes" / "character.tsv").read_text(encoding="utf-8")
    assert (subset / "graph" / "relationships" / "episode_of.tsv").read_text(encoding="utf-8").count("tt0002") == 1

    subprocess.run([
        sys.executable, str(EXAMPLE_DIR / "build_subset.py"), "--input-dir", str(source),
        "--output-dir", str(output), "--titles", "1", "--offset", "1", "--depth", "0",
    ], check=True)
    offset_subset = output / "titles-1-offset-1-depth-0"
    offset_titles = list(csv.DictReader((offset_subset / "graph" / "nodes" / "title.tsv").open(), delimiter="\t"))
    assert [row["tconst"] for row in offset_titles] == ["tt0002"]
    assert json.loads((offset_subset / "manifest.json").read_text(encoding="utf-8"))["seed_offset"] == 1


def test_seed_offset_counts_only_eligible_titles_across_chunks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    basics = tmp_path / "title.basics.tsv"
    write_tsv(basics, ["tconst", "titleType", "primaryTitle", "isAdult"], [
        ["tt0001", "movie", "One", "0"],
        ["tt0002", "movie", "Adult", "1"],
        ["tt0003", r"\N", "No type", "0"],
        ["tt0004", "movie", "Four", "0"],
        ["tt0005", "movie", r"\N", "0"],
        ["tt0006", "movie", "Six", "0"],
        ["tt0007", "movie", "Seven", "0"],
    ])
    monkeypatch.setattr(build_subset, "CHUNK_ROWS", 2)

    def select(offset: int, *, include_adult: bool = False) -> set[str]:
        return build_subset.select_seed_titles(
            basics, 2, include_adult, offset=offset, verbose=False
        )

    assert select(0) == {"tt0001", "tt0004"}
    assert select(1) == {"tt0004", "tt0006"}
    assert select(2) == {"tt0006", "tt0007"}
    assert select(1, include_adult=True) == {"tt0002", "tt0004"}
    with pytest.raises(ValueError, match="after offset 3"):
        select(3)
