#!/usr/bin/env python3
"""Build a bounded, breadth-first IMDb graph subset from unmodified TSV input.

``--titles N`` selects the first N eligible titles in ``title.basics.tsv``.
``--depth`` is the number of Title↔Person credit hops to traverse from those
seeds.  Title and person budgets keep a small seed from expanding into the
entire IMDb catalogue; pass explicit budgets for larger demonstrations.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
import re
import time
from typing import Callable, Iterable

import pandas as pd


REQUIRED_FILES = (
    "name.basics.tsv",
    "title.akas.tsv",
    "title.basics.tsv",
    "title.crew.tsv",
    "title.episode.tsv",
    "title.principals.tsv",
    "title.ratings.tsv",
)
MISSING = r"\N"
CHUNK_ROWS = 500_000
logger = logging.getLogger(__name__)


def rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as source:
        yield from csv.DictReader(source, delimiter="\t")


def tsv_chunks(
    path: Path,
    field_names: list[str] | None,
    phase: str,
    *,
    verbose: bool,
    ordered_key: str | None = None,
    stop_after: str | None = None,
) -> Iterable[pd.DataFrame]:
    """Read large TSVs with Pandas' C parser in bounded, vectorized chunks."""
    started = time.monotonic()
    scanned = 0
    reader = pd.read_csv(
        path,
        sep="\t",
        usecols=field_names,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        chunksize=CHUNK_ROWS,
    )
    for chunk_number, frame in enumerate(reader, start=1):
        scanned += len(frame)
        if verbose:
            elapsed = time.monotonic() - started
            rate = scanned / elapsed if elapsed else 0
            logger.info(
                "%s: chunk %s, scanned %s rows in %.1fs (%.0f rows/s)",
                phase, chunk_number, f"{scanned:,}", elapsed, rate,
            )
        yield frame
        # IMDb's name/title keyed exports are ordered by their identifier.  If
        # a caller only needs early IDs, every subsequent chunk is irrelevant.
        # This is a major win for a small n without changing selected records.
        if ordered_key and stop_after and not frame.empty and frame[ordered_key].iloc[-1] > stop_after:
            logger.info("%s: passed ordered %s boundary %s after %s rows", phase, ordered_key, stop_after, f"{scanned:,}")
            break
    elapsed = time.monotonic() - started
    logger.info("%s: completed %s rows in %.1fs", phase, f"{scanned:,}", elapsed)


def present(value: str | None) -> str:
    """Turn IMDb's missing sentinel into an empty value for generated graph TSVs."""
    return "" if value in (None, "", MISSING) else value


def split_imdb_list(value: str | None) -> list[str]:
    value = present(value)
    return [] if not value else [item for item in value.split(",") if item]


def add_limited(target: set[str], candidates: Iterable[str], limit: int, new: set[str]) -> None:
    """Add source-order candidates to ``target`` up to its hard budget."""
    for candidate in candidates:
        if candidate and candidate not in target and len(target) < limit:
            target.add(candidate)
            new.add(candidate)


def discover_people(input_dir: Path, title_ids: set[str], limit: int, selected: set[str], *, verbose: bool) -> set[str]:
    """Find credited people for the supplied title frontier in source-file order."""
    new: set[str] = set()
    last_title = max(title_ids)
    for frame in tsv_chunks(
        input_dir / "title.principals.tsv", ["tconst", "nconst"], "principal credits for title frontier",
        verbose=verbose, ordered_key="tconst", stop_after=last_title,
    ):
        add_limited(selected, frame.loc[frame["tconst"].isin(title_ids), "nconst"], limit, new)
        if len(selected) == limit:
            return new
    for frame in tsv_chunks(
        input_dir / "title.crew.tsv", ["tconst", "directors", "writers"], "crew credits for title frontier",
        verbose=verbose, ordered_key="tconst", stop_after=last_title,
    ):
        matches = frame.loc[frame["tconst"].isin(title_ids)]
        for column in ("directors", "writers"):
            for value in matches[column]:
                add_limited(selected, split_imdb_list(value), limit, new)
                if len(selected) == limit:
                    return new
    return new


def discover_titles(input_dir: Path, person_ids: set[str], limit: int, selected: set[str], *, verbose: bool) -> set[str]:
    """Find titles credited to the supplied person frontier in source-file order."""
    new: set[str] = set()
    for frame in tsv_chunks(input_dir / "title.principals.tsv", ["tconst", "nconst"], "principal credits for person frontier", verbose=verbose):
        add_limited(selected, frame.loc[frame["nconst"].isin(person_ids), "tconst"], limit, new)
        if len(selected) == limit:
            return new

    # IDs are comma-delimited in the crew TSV.  One compiled vectorized regex
    # avoids Python work per row while retaining exact ID-boundary matching.
    people_pattern = re.compile(r"(?:^|,)(?:" + "|".join(re.escape(item) for item in person_ids) + r")(?:,|$)")
    for frame in tsv_chunks(input_dir / "title.crew.tsv", ["tconst", "directors", "writers"], "crew credits for person frontier", verbose=verbose):
        match = frame["directors"].str.contains(people_pattern, na=False) | frame["writers"].str.contains(people_pattern, na=False)
        add_limited(selected, frame.loc[match, "tconst"], limit, new)
        if len(selected) == limit:
            return new
    return new


class TsvSink:
    """Write one generated TSV with a stable header."""

    def __init__(self, path: Path, fields: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("w", encoding="utf-8", newline="")
        self._writer = csv.DictWriter(self._file, fieldnames=fields, delimiter="\t", lineterminator="\n", extrasaction="ignore")
        self._writer.writeheader()

    def write(self, **row: str) -> None:
        self._writer.writerow({field: present(row.get(field)) for field in self._writer.fieldnames})

    def close(self) -> None:
        self._file.close()


def copy_filtered(
    source: Path,
    destination: Path,
    include: Callable[[pd.DataFrame], pd.Series],
    *,
    verbose: bool,
    ordered_key: str | None = None,
    stop_after: str | None = None,
) -> None:
    """Write a schema/value-preserving filtered TSV using vectorized Pandas masks.

    The full extracted TSVs are intentionally never modified.  Subset copies
    retain their official columns and values, while using normalized TSV output.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    started, copied = time.monotonic(), 0
    first_chunk = True
    for frame in tsv_chunks(
        source, None, f"copy {source.name}", verbose=verbose, ordered_key=ordered_key, stop_after=stop_after
    ):
        kept = frame.loc[include(frame)]
        copied += len(kept)
        kept.to_csv(destination, sep="\t", index=False, mode="w" if first_chunk else "a", header=first_chunk, lineterminator="\n")
        first_chunk = False
    if first_chunk:  # Defensive fallback for a header-only input file.
        pd.read_csv(source, sep="\t", dtype=str, keep_default_na=False, na_filter=False, nrows=0).to_csv(destination, sep="\t", index=False, lineterminator="\n")
    logger.info("copy %s: kept %s row(s) in %.1fs", source.name, f"{copied:,}", time.monotonic() - started)


def source_subset(input_dir: Path, destination: Path, titles: set[str], people: set[str], *, verbose: bool) -> None:
    """Create filtered copies retaining the official IMDb TSV schemas."""
    last_title, last_person = max(titles), max(people) if people else None
    copy_filtered(input_dir / "title.basics.tsv", destination / "title.basics.tsv", lambda frame: frame["tconst"].isin(titles), verbose=verbose, ordered_key="tconst", stop_after=last_title)
    copy_filtered(input_dir / "name.basics.tsv", destination / "name.basics.tsv", lambda frame: frame["nconst"].isin(people), verbose=verbose, ordered_key="nconst", stop_after=last_person)
    copy_filtered(input_dir / "title.principals.tsv", destination / "title.principals.tsv", lambda frame: frame["tconst"].isin(titles) & frame["nconst"].isin(people), verbose=verbose, ordered_key="tconst", stop_after=last_title)
    copy_filtered(input_dir / "title.crew.tsv", destination / "title.crew.tsv", lambda frame: frame["tconst"].isin(titles), verbose=verbose, ordered_key="tconst", stop_after=last_title)
    copy_filtered(input_dir / "title.episode.tsv", destination / "title.episode.tsv", lambda frame: frame["tconst"].isin(titles) & frame["parentTconst"].isin(titles), verbose=verbose, ordered_key="tconst", stop_after=last_title)
    copy_filtered(input_dir / "title.akas.tsv", destination / "title.akas.tsv", lambda frame: frame["titleId"].isin(titles), verbose=verbose, ordered_key="titleId", stop_after=last_title)
    copy_filtered(input_dir / "title.ratings.tsv", destination / "title.ratings.tsv", lambda frame: frame["tconst"].isin(titles), verbose=verbose, ordered_key="tconst", stop_after=last_title)


def graph_subset(input_dir: Path, destination: Path, titles: set[str], people: set[str]) -> dict[str, int]:
    """Create graph-oriented node and relationship TSVs for the selected IDs."""
    node_dir, relationship_dir = destination / "nodes", destination / "relationships"
    sinks = {
        "title": TsvSink(node_dir / "title.tsv", ["tconst", "title_type", "primary_title", "original_title", "is_adult", "start_year", "end_year", "runtime_minutes"]),
        "person": TsvSink(node_dir / "person.tsv", ["nconst", "primary_name", "birth_year", "death_year"]),
        "genre": TsvSink(node_dir / "genre.tsv", ["name"]),
        "profession": TsvSink(node_dir / "profession.tsv", ["name"]),
        "character": TsvSink(node_dir / "character.tsv", ["name"]),
        "title_type": TsvSink(node_dir / "title_type.tsv", ["name"]),
        "region": TsvSink(node_dir / "region.tsv", ["code"]),
        "language": TsvSink(node_dir / "language.tsv", ["code"]),
        "alternative_title": TsvSink(node_dir / "alternative_title.tsv", ["aka_id", "title_id", "ordering", "title", "is_original_title"]),
        "rating": TsvSink(node_dir / "rating.tsv", ["rating_id", "title_id", "average_rating", "num_votes"]),
        "has_genre": TsvSink(relationship_dir / "has_genre.tsv", ["title_id", "genre"]),
        "has_title_type": TsvSink(relationship_dir / "has_title_type.tsv", ["title_id", "title_type"]),
        "has_profession": TsvSink(relationship_dir / "has_profession.tsv", ["person_id", "profession"]),
        "known_for": TsvSink(relationship_dir / "known_for.tsv", ["person_id", "title_id"]),
        "credited_on": TsvSink(relationship_dir / "credited_on.tsv", ["person_id", "title_id", "category", "ordering", "job"]),
        "acted_in": TsvSink(relationship_dir / "acted_in.tsv", ["person_id", "title_id", "category", "ordering"]),
        "played": TsvSink(relationship_dir / "played.tsv", ["person_id", "character", "title_id"]),
        "directed": TsvSink(relationship_dir / "directed.tsv", ["person_id", "title_id"]),
        "wrote": TsvSink(relationship_dir / "wrote.tsv", ["person_id", "title_id"]),
        "episode_of": TsvSink(relationship_dir / "episode_of.tsv", ["episode_id", "series_id", "season_number", "episode_number"]),
        "has_alternative_title": TsvSink(relationship_dir / "has_alternative_title.tsv", ["title_id", "aka_id"]),
        "available_in_region": TsvSink(relationship_dir / "available_in_region.tsv", ["aka_id", "region"]),
        "in_language": TsvSink(relationship_dir / "in_language.tsv", ["aka_id", "language"]),
        "has_rating": TsvSink(relationship_dir / "has_rating.tsv", ["title_id", "rating_id"]),
    }
    seen: dict[str, set[str]] = {key: set() for key in ("genre", "profession", "character", "title_type", "region", "language", "alternative_title", "rating")}

    def unique(kind: str, key: str, **row: str) -> None:
        if key and key not in seen[kind]:
            seen[kind].add(key)
            sinks[kind].write(**row)

    for row in rows(input_dir / "title.basics.tsv"):
        title_id = row["tconst"]
        if title_id not in titles:
            continue
        title_type = present(row["titleType"])
        sinks["title"].write(tconst=title_id, title_type=title_type, primary_title=row["primaryTitle"], original_title=row["originalTitle"], is_adult=row["isAdult"], start_year=row["startYear"], end_year=row["endYear"], runtime_minutes=row["runtimeMinutes"])
        unique("title_type", title_type, name=title_type)
        if title_type:
            sinks["has_title_type"].write(title_id=title_id, title_type=title_type)
        for genre in split_imdb_list(row["genres"]):
            unique("genre", genre, name=genre)
            sinks["has_genre"].write(title_id=title_id, genre=genre)

    for row in rows(input_dir / "name.basics.tsv"):
        person_id = row["nconst"]
        if person_id not in people:
            continue
        sinks["person"].write(nconst=person_id, primary_name=row["primaryName"], birth_year=row["birthYear"], death_year=row["deathYear"])
        for profession in split_imdb_list(row["primaryProfession"]):
            unique("profession", profession, name=profession)
            sinks["has_profession"].write(person_id=person_id, profession=profession)
        for title_id in split_imdb_list(row["knownForTitles"]):
            if title_id in titles:
                sinks["known_for"].write(person_id=person_id, title_id=title_id)

    for row in rows(input_dir / "title.principals.tsv"):
        title_id, person_id = row["tconst"], row["nconst"]
        if title_id not in titles or person_id not in people:
            continue
        category = present(row["category"])
        sinks["credited_on"].write(person_id=person_id, title_id=title_id, category=category, ordering=row["ordering"], job=row["job"])
        if category in {"actor", "actress", "self"}:
            sinks["acted_in"].write(person_id=person_id, title_id=title_id, category=category, ordering=row["ordering"])
        characters = present(row["characters"])
        if characters:
            try:
                names = json.loads(characters)
            except json.JSONDecodeError:
                names = []
            for name in names if isinstance(names, list) else []:
                if isinstance(name, str) and name:
                    unique("character", name, name=name)
                    sinks["played"].write(person_id=person_id, character=name, title_id=title_id)

    for row in rows(input_dir / "title.crew.tsv"):
        title_id = row["tconst"]
        if title_id not in titles:
            continue
        for person_id in split_imdb_list(row["directors"]):
            if person_id in people:
                sinks["directed"].write(person_id=person_id, title_id=title_id)
        for person_id in split_imdb_list(row["writers"]):
            if person_id in people:
                sinks["wrote"].write(person_id=person_id, title_id=title_id)

    for row in rows(input_dir / "title.episode.tsv"):
        if row["tconst"] in titles and row["parentTconst"] in titles:
            sinks["episode_of"].write(episode_id=row["tconst"], series_id=row["parentTconst"], season_number=row["seasonNumber"], episode_number=row["episodeNumber"])

    for row in rows(input_dir / "title.akas.tsv"):
        title_id = row["titleId"]
        if title_id not in titles:
            continue
        aka_id = f"{title_id}:{row['ordering']}"
        unique("alternative_title", aka_id, aka_id=aka_id, title_id=title_id, ordering=row["ordering"], title=row["title"], is_original_title=row["isOriginalTitle"])
        sinks["has_alternative_title"].write(title_id=title_id, aka_id=aka_id)
        region, language = present(row["region"]), present(row["language"])
        if region:
            unique("region", region, code=region)
            sinks["available_in_region"].write(aka_id=aka_id, region=region)
        if language:
            unique("language", language, code=language)
            sinks["in_language"].write(aka_id=aka_id, language=language)

    for row in rows(input_dir / "title.ratings.tsv"):
        title_id = row["tconst"]
        if title_id not in titles:
            continue
        rating_id = title_id
        unique("rating", rating_id, rating_id=rating_id, title_id=title_id, average_rating=row["averageRating"], num_votes=row["numVotes"])
        sinks["has_rating"].write(title_id=title_id, rating_id=rating_id)

    for sink in sinks.values():
        sink.close()
    return {"titles": len(titles), "people": len(people), **{kind: len(values) for kind, values in seen.items()}}


def select_seed_titles(path: Path, count: int, include_adult: bool, *, verbose: bool) -> set[str]:
    selected: set[str] = set()
    for frame in tsv_chunks(path, ["tconst", "titleType", "primaryTitle", "isAdult"], "seed-title selection", verbose=verbose):
        eligible = frame["titleType"].ne(MISSING) & frame["titleType"].ne("") & frame["primaryTitle"].ne(MISSING) & frame["primaryTitle"].ne("")
        if not include_adult:
            eligible &= frame["isAdult"].eq("0")
        for title_id in frame.loc[eligible, "tconst"]:
            selected.add(title_id)
            if len(selected) == count:
                return selected
    raise ValueError(f"only found {len(selected)} eligible titles; requested {count}")


def main() -> int:
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("examples/imdb/data/extracted"))
    parser.add_argument("--output-dir", type=Path, default=Path("examples/imdb/data/subsets"))
    parser.add_argument("--titles", type=int, required=True, help="number of seed titles")
    parser.add_argument("--depth", type=int, default=2, help="Title↔Person credit hops from the seed titles")
    parser.add_argument("--max-titles", type=int, help="final title budget (default: ten times --titles)")
    parser.add_argument("--max-people", type=int, help="final person budget (default: fifty times --titles)")
    parser.add_argument("--include-adult", action="store_true", help="allow adult seed titles")
    parser.add_argument("--verbose", action="store_true", help="log million-row progress and phase durations")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("Building IMDb subset from %s to %s with %s seed titles and depth %s", args.input_dir, args.output_dir, args.titles, args.depth)
    if args.titles < 1 or args.depth < 0:
        parser.error("--titles must be positive and --depth cannot be negative")
    missing = [name for name in REQUIRED_FILES if not (args.input_dir / name).is_file()]
    if missing:
        parser.error(f"missing extracted IMDb files: {', '.join(missing)}")

    title_limit = args.max_titles or args.titles * 10
    person_limit = args.max_people or args.titles * 50
    if title_limit < args.titles or person_limit < 1:
        parser.error("budgets must be positive and --max-titles cannot be below --titles")

    logger.info("Using title budget %s and person budget %s", title_limit, person_limit)
    titles = select_seed_titles(args.input_dir / "title.basics.tsv", args.titles, args.include_adult, verbose=args.verbose)
    logger.info("Selected %s seed title(s): %s", len(titles), ", ".join(sorted(titles)))
    people: set[str] = set()
    title_frontier, person_frontier = set(titles), set()
    for hop in range(args.depth):
        if hop % 2 == 0:
            logger.info("Depth %s/%s: discovering people for %s title(s)", hop + 1, args.depth, len(title_frontier))
            person_frontier = discover_people(args.input_dir, title_frontier, person_limit, people, verbose=args.verbose)
            title_frontier = set()
        else:
            logger.info("Depth %s/%s: discovering titles for %s person(s)", hop + 1, args.depth, len(person_frontier))
            title_frontier = discover_titles(args.input_dir, person_frontier, title_limit, titles, verbose=args.verbose)
            person_frontier = set()
        logger.info("After depth %s: selected %s title(s), %s person(s); next frontier has %s item(s)", hop + 1, len(titles), len(people), len(title_frontier or person_frontier))
        if not title_frontier and not person_frontier:
            break

    output = args.output_dir / f"titles-{args.titles}-depth-{args.depth}"
    logger.info("Writing source subset with %s title(s) and %s person(s) to %s", len(titles), len(people), output)
    source_subset(args.input_dir, output / "source", titles, people, verbose=args.verbose)
    # Graph derivation now scans only the tiny filtered source copies, rather
    # than re-reading the full 9+ GB extracted dataset a second time.
    logger.info("Deriving graph node and relationship TSVs from filtered source copies")
    counts = graph_subset(output / "source", output / "graph", titles, people)
    manifest = {
        "seed_titles": args.titles,
        "depth": args.depth,
        "title_budget": title_limit,
        "person_budget": person_limit,
        "selected": counts,
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    logger.info("Wrote subset to %s", output)
    logger.info("Selected graph records: %s", json.dumps(manifest["selected"], sort_keys=True))
    logger.info("IMDb subset build completed in %.1fs", time.monotonic() - started)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
