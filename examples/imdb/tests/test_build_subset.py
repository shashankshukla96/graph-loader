"""Regression test for the local IMDb subset builder using tiny official-shaped TSVs."""
from __future__ import annotations

import csv
import json
from pathlib import Path
import random
import subprocess
import sys

import numpy as np
import pandas as pd
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


# ---------------------------------------------------------------------------
# Full export (--all) and the shared vectorized graph derivation.

TITLE_FIELDS = ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"]
NAME_FIELDS = ["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"]
PRINCIPAL_FIELDS = ["tconst", "ordering", "nconst", "category", "job", "characters"]
AKA_FIELDS = ["titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"]


def write_tricky_dataset(source: Path) -> None:
    """Official-shaped TSVs covering every drop, dedup, quoting and parsing rule."""
    source.mkdir(parents=True, exist_ok=True)
    write_tsv(source / "title.basics.tsv", TITLE_FIELDS, [
        ["tt0001", "movie", 'The "Seed"', "Seed\tTab", "0", "2000", r"\N", "100", "Drama,Comedy"],
        ["tt0002", "tvSeries", "Series", "Series", "0", "2001", "2003", r"\N", "Drama,,Horror"],
        ["tt0003", "tvEpisode", "Episode", "Episode", "0", r"\N", r"\N", "30", r"\N"],
        ["tt0004", "movie", "Adult", "Adult", "1", "2004", r"\N", "90", "Adult"],
        ["tt0005", "tvEpisode", "Orphan episode", "Orphan", "0", "2005", r"\N", "20", "Comedy"],
        ["tt0006", "tvEpisode", "Adult parent", "Adult parent", "0", "2006", r"\N", "20", "Drama"],
        ["tt0007", r"\N", "Untyped", "Untyped", "0", "2007", r"\N", "10", ""],
        ["tt0008", "short", r"\N", "Unnamed", "0", "2008", r"\N", "5", "Short"],
    ])
    write_tsv(source / "name.basics.tsv", NAME_FIELDS, [
        ["nm0001", "Ada", "1970", r"\N", "actor,writer", "tt0001,tt0004,tt9999"],
        ["nm0002", 'Grace "G"', r"\N", r"\N", "director,,actor", "tt0002"],
        ["nm0003", "Line\nBreak", "1980", "2020", r"\N", r"\N"],
        ["nm0004", "Adult actor", "1990", r"\N", "actress", "tt0004"],
    ])
    write_tsv(source / "title.principals.tsv", PRINCIPAL_FIELDS, [
        ["tt0001", "1", "nm0001", "actor", r"\N", '["Hero","Villain"]'],
        ["tt0001", "2", "nm9999", "actor", r"\N", '["Ghost"]'],
        ["tt0001", "3", "nm0002", "self", r"\N", '["Hero"]'],
        ["tt0002", "1", "nm0001", "writer", "creator", r"\N"],
        ["tt0002", "2", "nm0003", "actress", r"\N", '["Esc\\"aped \\u00e9"]'],
        ["tt0002", "3", "nm0003", "actress", r"\N", '["tab\there"]'],
        ["tt0002", "4", "nm0002", "actor", r"\N", "not json"],
        ["tt0003", "1", "nm0002", "director", r"\N", '[""]'],
        ["tt0003", "2", "nm0001", "actor", r"\N", '[1, "Numbered", ""]'],
        ["tt0003", "3", "nm0003", "actor", r"\N", '"Just a string"'],
        ["tt0004", "1", "nm0004", "actress", r"\N", '["Adult role"]'],
        ["tt9999", "1", "nm0001", "actor", r"\N", '["Missing title"]'],
    ])
    write_tsv(source / "title.crew.tsv", ["tconst", "directors", "writers"], [
        ["tt0001", "nm0002,nm9999", "nm0001"], ["tt0002", r"\N", "nm0001,,nm0003"], ["tt0004", "nm0004", r"\N"], ["tt9999", "nm0001", "nm0001"],
    ])
    write_tsv(source / "title.episode.tsv", ["tconst", "parentTconst", "seasonNumber", "episodeNumber"], [
        ["tt0003", "tt0002", "1", "1"], ["tt0005", "tt9998", "1", "2"], ["tt0006", "tt0004", r"\N", r"\N"],
    ])
    write_tsv(source / "title.akas.tsv", AKA_FIELDS, [
        ["tt0001", "1", "Seed US", "US", "en", r"\N", r"\N", "0"],
        ["tt0001", "2", "Seed FR", "FR", r"\N", r"\N", r"\N", "0"],
        ["tt0001", "2", "Seed FR duplicate", "FR", "fr", r"\N", r"\N", "0"],
        ["tt0002", "1", "Série", r"\N", "fr", r"\N", r"\N", "1"],
        ["tt0004", "1", "Adult aka", "US", "en", r"\N", r"\N", "0"],
        ["tt9999", "1", "Missing", "DE", "de", r"\N", r"\N", "0"],
    ])
    write_tsv(source / "title.ratings.tsv", ["tconst", "averageRating", "numVotes"], [
        ["tt0001", "7.5", "10"], ["tt0001", "7.6", "11"], ["tt0004", "5.0", "3"], ["tt9999", "1.0", "1"],
    ])


def read_graph(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in sorted(root.rglob("*.tsv"))}


def graph_rows(root: Path, relative: str) -> list[dict[str, str]]:
    with (root / relative).open(encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source, delimiter="\t"))


def open_sink(path: Path, fields: tuple[str, ...]):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w", encoding="utf-8", newline="")
    writer = csv.DictWriter(handle, fieldnames=list(fields), delimiter="\t", lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    return handle, writer


def reference_graph_subset(input_dir: Path, destination: Path, titles: set[str], people: set[str]) -> None:
    """The original row-by-row ``graph_subset``: the oracle for the vectorized derivation."""
    present, split = build_subset.present, build_subset.split_imdb_list

    def rows(path: Path):
        with path.open("r", encoding="utf-8", newline="") as source:
            yield from csv.DictReader(source, delimiter="\t")

    files = {kind: open_sink(destination / relative, fields) for kind, (relative, fields) in build_subset.GRAPH_FILES.items()}
    seen: dict[str, set[str]] = {kind: set() for kind in build_subset.DEDUPLICATED_KINDS}

    def write(kind: str, **row: str) -> None:
        files[kind][1].writerow({field: present(row.get(field)) for field in files[kind][1].fieldnames})

    def unique(kind: str, key: str, **row: str) -> None:
        if key and key not in seen[kind]:
            seen[kind].add(key)
            write(kind, **row)

    titles = {row["tconst"] for row in rows(input_dir / "title.basics.tsv") if row["tconst"] in titles}
    people = {row["nconst"] for row in rows(input_dir / "name.basics.tsv") if row["nconst"] in people}
    for row in rows(input_dir / "title.basics.tsv"):
        if row["tconst"] not in titles:
            continue
        title_type = present(row["titleType"])
        write("title", tconst=row["tconst"], title_type=title_type, primary_title=row["primaryTitle"], original_title=row["originalTitle"], is_adult=row["isAdult"], start_year=row["startYear"], end_year=row["endYear"], runtime_minutes=row["runtimeMinutes"])
        unique("title_type", title_type, name=title_type)
        if title_type:
            write("has_title_type", title_id=row["tconst"], title_type=title_type)
        for genre in split(row["genres"]):
            unique("genre", genre, name=genre)
            write("has_genre", title_id=row["tconst"], genre=genre)
    for row in rows(input_dir / "name.basics.tsv"):
        if row["nconst"] not in people:
            continue
        write("person", nconst=row["nconst"], primary_name=row["primaryName"], birth_year=row["birthYear"], death_year=row["deathYear"])
        for profession in split(row["primaryProfession"]):
            unique("profession", profession, name=profession)
            write("has_profession", person_id=row["nconst"], profession=profession)
        for title_id in split(row["knownForTitles"]):
            if title_id in titles:
                write("known_for", person_id=row["nconst"], title_id=title_id)
    for row in rows(input_dir / "title.principals.tsv"):
        title_id, person_id = row["tconst"], row["nconst"]
        if title_id not in titles or person_id not in people:
            continue
        category = present(row["category"])
        write("credited_on", person_id=person_id, title_id=title_id, category=category, ordering=row["ordering"], job=row["job"])
        if category in {"actor", "actress", "self"}:
            write("acted_in", person_id=person_id, title_id=title_id, category=category, ordering=row["ordering"])
        characters = present(row["characters"])
        if characters:
            try:
                names = json.loads(characters)
            except json.JSONDecodeError:
                names = []
            for name in names if isinstance(names, list) else []:
                if isinstance(name, str) and name:
                    unique("character", name, name=name)
                    write("played", person_id=person_id, character=name, title_id=title_id)
    for row in rows(input_dir / "title.crew.tsv"):
        if row["tconst"] not in titles:
            continue
        for person_id in split(row["directors"]):
            if person_id in people:
                write("directed", person_id=person_id, title_id=row["tconst"])
        for person_id in split(row["writers"]):
            if person_id in people:
                write("wrote", person_id=person_id, title_id=row["tconst"])
    for row in rows(input_dir / "title.episode.tsv"):
        if row["tconst"] in titles and row["parentTconst"] in titles:
            write("episode_of", episode_id=row["tconst"], series_id=row["parentTconst"], season_number=row["seasonNumber"], episode_number=row["episodeNumber"])
    for row in rows(input_dir / "title.akas.tsv"):
        title_id = row["titleId"]
        if title_id not in titles:
            continue
        aka_id = f"{title_id}:{row['ordering']}"
        unique("alternative_title", aka_id, aka_id=aka_id, title_id=title_id, ordering=row["ordering"], title=row["title"], is_original_title=row["isOriginalTitle"])
        write("has_alternative_title", title_id=title_id, aka_id=aka_id)
        region, language = present(row["region"]), present(row["language"])
        if region:
            unique("region", region, code=region)
            write("available_in_region", aka_id=aka_id, region=region)
        if language:
            unique("language", language, code=language)
            write("in_language", aka_id=aka_id, language=language)
    for row in rows(input_dir / "title.ratings.tsv"):
        if row["tconst"] in titles:
            unique("rating", row["tconst"], rating_id=row["tconst"], title_id=row["tconst"], average_rating=row["averageRating"], num_votes=row["numVotes"])
            write("has_rating", title_id=row["tconst"], rating_id=row["tconst"])
    for handle, _ in files.values():
        handle.close()


@pytest.mark.parametrize("jobs", [1, 3])
def test_vectorized_derivation_matches_row_by_row_reference(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, jobs: int) -> None:
    source = tmp_path / "source"
    write_tricky_dataset(source)
    monkeypatch.setattr(build_subset, "CHUNK_ROWS", 2)  # dedup and ordering must hold across chunks
    titles = {"tt0001", "tt0002", "tt0003", "tt0004", "tt0005", "tt9999"}
    people = {"nm0001", "nm0002", "nm0003", "nm9999"}
    reference_graph_subset(source, tmp_path / "reference", titles, people)
    counts = build_subset.graph_subset(source, tmp_path / "vectorized", titles, people, jobs=jobs)
    assert read_graph(tmp_path / "vectorized") == read_graph(tmp_path / "reference")
    assert counts["titles"] == 5 and counts["people"] == 3  # tt9999 / nm9999 are not in the basics files
    assert counts["alternative_title"] == 4 and counts["rating"] == 2


def run_all(source: Path, output: Path, *extra: str) -> None:
    subprocess.run([
        sys.executable, str(EXAMPLE_DIR / "build_subset.py"), "--input-dir", str(source), "--output-dir", str(output), "--all", *extra,
    ], check=True)


def test_all_mode_drops_every_relationship_whose_endpoint_is_not_a_node(tmp_path: Path) -> None:
    source = tmp_path / "source"
    write_tricky_dataset(source)
    run_all(source, tmp_path / "out", "--jobs", "1")
    export = tmp_path / "out" / "all"
    graph = export / "graph"
    assert not (export / "source").exists()
    # Seed eligibility: typed, named and non-adult.
    assert [row["tconst"] for row in graph_rows(graph, "nodes/title.tsv")] == ["tt0001", "tt0002", "tt0003", "tt0005", "tt0006"]
    assert [row["nconst"] for row in graph_rows(graph, "nodes/person.tsv")] == ["nm0001", "nm0002", "nm0003", "nm0004"]
    title_ids = {row["tconst"] for row in graph_rows(graph, "nodes/title.tsv")}
    person_ids = {row["nconst"] for row in graph_rows(graph, "nodes/person.tsv")}
    for relationship in ("known_for", "credited_on", "acted_in", "played", "directed", "wrote"):
        for row in graph_rows(graph, f"relationships/{relationship}.tsv"):
            assert row["person_id"] in person_ids and row["title_id"] in title_ids, (relationship, row)
    assert [(row["person_id"], row["title_id"]) for row in graph_rows(graph, "relationships/known_for.tsv")] == [("nm0001", "tt0001"), ("nm0002", "tt0002")]
    assert [(row["person_id"], row["title_id"]) for row in graph_rows(graph, "relationships/directed.tsv")] == [("nm0002", "tt0001")]
    assert [row["episode_id"] for row in graph_rows(graph, "relationships/episode_of.tsv")] == ["tt0003"]
    assert [row["aka_id"] for row in graph_rows(graph, "nodes/alternative_title.tsv")] == ["tt0001:1", "tt0001:2", "tt0002:1"]
    assert [row["aka_id"] for row in graph_rows(graph, "relationships/has_alternative_title.tsv")] == ["tt0001:1", "tt0001:2", "tt0001:2", "tt0002:1"]
    assert [row["name"] for row in graph_rows(graph, "nodes/character.tsv")] == ["Hero", "Villain", 'Esc"aped é', "Numbered"]
    assert [row["rating_id"] for row in graph_rows(graph, "nodes/rating.tsv")] == ["tt0001"]
    assert graph_rows(graph, "nodes/title.tsv")[0]["original_title"] == "Seed\tTab"
    assert graph_rows(graph, "nodes/person.tsv")[2]["primary_name"] == "Line\nBreak"
    manifest = json.loads((export / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["mode"] == "all" and manifest["include_adult"] is False and manifest["source_copies"] is False
    assert manifest["selected"]["titles"] == 5 and manifest["selected"]["people"] == 4
    assert manifest["records"]["relationships/credited_on.tsv"] == len(graph_rows(graph, "relationships/credited_on.tsv")) == 9
    assert set(manifest["records"]) == {relative for relative, _ in build_subset.GRAPH_FILES.values()}

    run_all(source, tmp_path / "out", "--include-adult", "--jobs", "2")
    adult = tmp_path / "out" / "all-with-adult" / "graph"
    assert "tt0004" in {row["tconst"] for row in graph_rows(adult, "nodes/title.tsv")}
    assert ("tt0006", "tt0004") in {(row["episode_id"], row["series_id"]) for row in graph_rows(adult, "relationships/episode_of.tsv")}
    assert ("nm0004", "tt0004") in {(row["person_id"], row["title_id"]) for row in graph_rows(adult, "relationships/known_for.tsv")}
    assert json.loads((tmp_path / "out" / "all-with-adult" / "manifest.json").read_text(encoding="utf-8"))["include_adult"] is True


def test_all_mode_equals_a_subset_that_selects_every_title_and_person(tmp_path: Path) -> None:
    source = tmp_path / "source"
    write_tricky_dataset(source)
    # Drop the ineligible titles, so an unbounded traversal from every seed reaches exactly --all's selection.
    basics = [line for line in (source / "title.basics.tsv").read_text(encoding="utf-8").splitlines(keepends=True) if not line.startswith(("tt0007", "tt0008"))]
    (source / "title.basics.tsv").write_text("".join(basics), encoding="utf-8")
    run_all(source, tmp_path / "out", "--include-adult")
    subprocess.run([
        sys.executable, str(EXAMPLE_DIR / "build_subset.py"), "--input-dir", str(source), "--output-dir", str(tmp_path / "out"),
        "--titles", "6", "--depth", "6", "--include-adult", "--max-titles", "100", "--max-people", "100",
    ], check=True)
    subset = tmp_path / "out" / "titles-6-depth-6"
    assert json.loads((subset / "manifest.json").read_text(encoding="utf-8"))["selected"]["people"] == 4
    assert read_graph(subset / "graph") == read_graph(tmp_path / "out" / "all-with-adult" / "graph")


def test_all_mode_rejects_subset_options(tmp_path: Path) -> None:
    result = subprocess.run([
        sys.executable, str(EXAMPLE_DIR / "build_subset.py"), "--input-dir", str(tmp_path), "--all", "--titles", "5",
    ], capture_output=True, text=True)
    assert result.returncode == 2 and "cannot be combined" in result.stderr
    result = subprocess.run([sys.executable, str(EXAMPLE_DIR / "build_subset.py"), "--input-dir", str(tmp_path)], capture_output=True, text=True)
    assert result.returncode == 2 and "--titles is required" in result.stderr


def test_key_set_matches_a_python_set_across_many_runs() -> None:
    generator = random.Random(7)
    keys = build_subset.KeySet()
    reference: set[str] = set()
    for _ in range(40):
        batch = [f"k{generator.randrange(3000)}" for _ in range(generator.randrange(0, 200))]
        expected = []
        for key in batch:
            expected.append(key not in reference)
            reference.add(key)
        assert keys.add(batch).tolist() == expected
    assert len(keys) == len(reference)
    probe = [f"k{value}" for value in range(3100)]
    assert keys.contains(probe).tolist() == [key in reference for key in probe]
    keys.freeze()
    assert keys.contains(probe).tolist() == [key in reference for key in probe]


def test_key_set_distinguishes_keys_whose_first_digest_collides(monkeypatch: pytest.MonkeyPatch) -> None:
    def colliding_digest(keys):
        values = np.asarray(keys, dtype=object)
        return np.zeros(len(values), dtype=np.uint64), np.array([ord(value[0]) for value in values], dtype=np.uint64)

    monkeypatch.setattr(build_subset.KeySet, "digest", staticmethod(colliding_digest))
    keys = build_subset.KeySet()
    assert keys.add(["a", "b", "a"]).tolist() == [True, True, False]
    assert keys.add(["c", "b"]).tolist() == [True, False]
    assert keys.contains(["a", "b", "c", "d"]).tolist() == [True, True, True, False]


def test_character_parsing_matches_json_semantics() -> None:
    values = ['["a"]', r"\N", "", '["b","c"]', '["x\\"y"]', '[""]', "bad", '"s"', '["a\tb"]', '[1,"q"]', '["\\u00e9"]', "[]", '["é ✓"]', '["a"', '{"a": 1}']
    positions, names = build_subset.explode_characters(pd.Series(values, dtype=str))
    expected = [(position, name) for position, value in enumerate(values) if build_subset.present(value) for name in build_subset.json_character_names(value)]
    assert list(zip(positions.tolist(), names.tolist())) == expected


def test_parallel_derivation_fails_closed_when_a_source_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_tricky_dataset(tmp_path / "source")

    def broken(*_args) -> None:
        raise ValueError("broken crew chunk")

    _, columns, kinds = build_subset.SOURCES["title.crew.tsv"]
    monkeypatch.setitem(build_subset.SOURCES, "title.crew.tsv", (broken, columns, kinds))
    with pytest.raises(RuntimeError, match="(?s)title.crew.tsv.*broken crew chunk"):
        build_subset.graph_subset(tmp_path / "source", tmp_path / "graph", {"tt0001"}, {"nm0001"}, jobs=2)


def test_tsv_sink_quotes_exactly_like_csv_dict_writer(tmp_path: Path) -> None:
    values = ["plain", 'quote"inside', "tab\there", "new\nline", "cr\rhere", "", r"\N", " lead", '"', "é"]
    sink = build_subset.TsvSink(tmp_path / "vectorized.tsv", ["a", "b"])
    sink.write_columns(values, list(reversed(values)))
    sink.close()
    handle, writer = open_sink(tmp_path / "reference.tsv", ("a", "b"))
    for first, second in zip(values, reversed(values)):
        writer.writerow({"a": build_subset.present(first), "b": build_subset.present(second)})
    handle.close()
    assert (tmp_path / "vectorized.tsv").read_bytes() == (tmp_path / "reference.tsv").read_bytes()
