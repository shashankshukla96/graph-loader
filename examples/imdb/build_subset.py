#!/usr/bin/env python3
"""Build a bounded, breadth-first IMDb graph subset -- or the whole dataset -- from unmodified TSV input.

``--titles N`` selects N eligible titles in ``title.basics.tsv`` after
``--offset`` eligible titles have been skipped.
``--depth`` is the number of Title↔Person credit hops to traverse from those
seeds.  Title and person budgets keep a small seed from expanding into the
entire IMDb catalogue; pass explicit budgets for larger demonstrations.

``--all`` instead converts the entire dataset: every eligible title (the same
rule as seed selection, so adult titles only with ``--include-adult``) and
every person.  Its graph files are derived exactly as a subset's are.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import multiprocessing
from multiprocessing.connection import wait as wait_for_connections
from pathlib import Path
import re
import time
import traceback
from typing import Callable, Iterable, Sequence

import numpy as np
import pandas as pd
from pandas.util import hash_array


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


# ---------------------------------------------------------------------------
# Graph derivation, shared by bounded subsets and ``--all``.
#
# Every source file is streamed in Pandas chunks and turned into graph rows
# with vectorized operations.  The rules are those of the original row-by-row
# derivation: IMDb's ``\N`` becomes an empty value in every output column;
# comma lists drop empty items; a relationship is written only when both of
# its endpoints are node records; deduplicated node kinds keep the first
# occurrence in source order; title and person rows are not deduplicated.

GRAPH_FILES: dict[str, tuple[str, tuple[str, ...]]] = {
    "title": ("nodes/title.tsv", ("tconst", "title_type", "primary_title", "original_title", "is_adult", "start_year", "end_year", "runtime_minutes")),
    "person": ("nodes/person.tsv", ("nconst", "primary_name", "birth_year", "death_year")),
    "genre": ("nodes/genre.tsv", ("name",)),
    "profession": ("nodes/profession.tsv", ("name",)),
    "character": ("nodes/character.tsv", ("name",)),
    "title_type": ("nodes/title_type.tsv", ("name",)),
    "region": ("nodes/region.tsv", ("code",)),
    "language": ("nodes/language.tsv", ("code",)),
    "alternative_title": ("nodes/alternative_title.tsv", ("aka_id", "title_id", "ordering", "title", "is_original_title")),
    "rating": ("nodes/rating.tsv", ("rating_id", "title_id", "average_rating", "num_votes")),
    "has_genre": ("relationships/has_genre.tsv", ("title_id", "genre")),
    "has_title_type": ("relationships/has_title_type.tsv", ("title_id", "title_type")),
    "has_profession": ("relationships/has_profession.tsv", ("person_id", "profession")),
    "known_for": ("relationships/known_for.tsv", ("person_id", "title_id")),
    "credited_on": ("relationships/credited_on.tsv", ("person_id", "title_id", "category", "ordering", "job")),
    "acted_in": ("relationships/acted_in.tsv", ("person_id", "title_id", "category", "ordering")),
    "played": ("relationships/played.tsv", ("person_id", "character", "title_id")),
    "directed": ("relationships/directed.tsv", ("person_id", "title_id")),
    "wrote": ("relationships/wrote.tsv", ("person_id", "title_id")),
    "episode_of": ("relationships/episode_of.tsv", ("episode_id", "series_id", "season_number", "episode_number")),
    "has_alternative_title": ("relationships/has_alternative_title.tsv", ("title_id", "aka_id")),
    "available_in_region": ("relationships/available_in_region.tsv", ("aka_id", "region")),
    "in_language": ("relationships/in_language.tsv", ("aka_id", "language")),
    "has_rating": ("relationships/has_rating.tsv", ("title_id", "rating_id")),
}
DEDUPLICATED_KINDS = ("genre", "profession", "character", "title_type", "region", "language", "alternative_title", "rating")
ACTING_CATEGORIES = ("actor", "actress", "self")
# A one-name ``characters`` value whose JSON string has no escapes or control
# characters decodes to exactly the text between the brackets and quotes.
# Nearly every IMDb value has this shape; the rest go through ``json.loads``.
SINGLE_CHARACTER = r'\["[^"\\\x00-\x1f]*"\]'
_single_character = re.compile(SINGLE_CHARACTER).fullmatch


class KeySet:
    """A set of strings held as 128-bit digests in sorted NumPy runs.

    A Python ``set`` of IMDb's 59.5M alternative-title IDs needs about 6 GB;
    this needs 16 bytes per key.  Each key is two independently keyed 64-bit
    SipHash digests, so two distinct keys are confused with probability about
    ``n**2 / 2**129`` (below 1e-20 for every IMDb key kind).  Sorted runs are
    merged like a binary counter, so a lookup probes O(log n) runs.
    """

    _HASH_KEYS = ("imdb-graph-key-1", "imdb-graph-key-2")

    def __init__(self) -> None:
        self._runs: list[tuple[np.ndarray, np.ndarray]] = []
        self._size = 0

    def __len__(self) -> int:
        return self._size

    @classmethod
    def digest(cls, keys: Sequence[str] | np.ndarray | pd.Series) -> tuple[np.ndarray, np.ndarray]:
        values = np.asarray(keys, dtype=object)
        first, second = (hash_array(values, hash_key=key) for key in cls._HASH_KEYS)
        return first, second

    def contains(self, keys: Sequence[str] | np.ndarray | pd.Series) -> np.ndarray:
        return self._contains(*self.digest(keys))

    def add(self, keys: Sequence[str] | np.ndarray | pd.Series) -> np.ndarray:
        """Add ``keys``; return the mask of first occurrences of keys not already present."""
        first, second = self.digest(keys)
        new = ~self._contains(first, second)
        if new.any():
            new &= ~pd.DataFrame({"first": first, "second": second}).duplicated().to_numpy()
            self._push(first[new], second[new])
        return new

    def freeze(self) -> None:
        """Merge every run into one before a read-only phase of lookups."""
        if len(self._runs) > 1:
            self._runs = [self._merge(self._runs)]

    def _contains(self, first: np.ndarray, second: np.ndarray) -> np.ndarray:
        found = np.zeros(len(first), dtype=bool)
        for run_first, run_second in self._runs:
            low = np.searchsorted(run_first, first, side="left")
            high = np.searchsorted(run_first, first, side="right")
            single = np.flatnonzero(high - low == 1)
            found[single] |= run_second[low[single]] == second[single]
            for position in np.flatnonzero(high - low > 1):  # a 64-bit collision inside this run
                found[position] |= bool((run_second[low[position]:high[position]] == second[position]).any())
        return found

    def _push(self, first: np.ndarray, second: np.ndarray) -> None:
        self._runs.append(self._merge([(first, second)]))
        self._size += len(first)
        while len(self._runs) > 1 and len(self._runs[-2][0]) <= len(self._runs[-1][0]):
            self._runs[-2:] = [self._merge(self._runs[-2:])]

    @staticmethod
    def _merge(runs: Sequence[tuple[np.ndarray, np.ndarray]]) -> tuple[np.ndarray, np.ndarray]:
        first = np.concatenate([run[0] for run in runs])
        second = np.concatenate([run[1] for run in runs])
        order = np.argsort(first, kind="stable")  # merges presorted runs in linear time
        return first[order], second[order]


def present_values(values: Sequence[str] | np.ndarray | pd.Series) -> np.ndarray:
    """Vectorized :func:`present` returning an object array."""
    array = np.array(values, dtype=object)
    array[array == MISSING] = ""
    return array


def explode_list(owners: np.ndarray, values: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """``(owner, item)`` pairs for every :func:`split_imdb_list` item, in source order."""
    items = pd.Series(present_values(values), dtype=object).str.split(",").explode()
    keep = items.notna().to_numpy() & (items.to_numpy(dtype=object) != "")
    return owners[items.index.to_numpy()[keep]], items.to_numpy(dtype=object)[keep]


def json_character_names(value: str) -> list[str]:
    try:
        names = json.loads(value)
    except json.JSONDecodeError:
        names = []
    return [name for name in names if isinstance(name, str) and name] if isinstance(names, list) else []


def explode_characters(values: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """``(row position, character name)`` for each ``characters`` JSON list entry, in source order."""
    characters = present_values(values)
    has_value = characters != ""
    single = np.fromiter((_single_character(value) is not None for value in characters), dtype=bool, count=len(characters))
    parsed = np.full(len(characters), None, dtype=object)
    parsed[single] = [value[2:-2] for value in characters[single]]
    for position in np.flatnonzero(has_value & ~single):
        parsed[position] = json_character_names(characters[position])
    names = pd.Series(parsed, dtype=object).explode()
    keep = names.notna().to_numpy() & (names.to_numpy(dtype=object) != "")
    return names.index.to_numpy()[keep], names.to_numpy(dtype=object)[keep]


class TsvSink:
    """Write one generated TSV with a stable header (``csv`` module quoting)."""

    def __init__(self, path: Path, fields: Sequence[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.fields = tuple(fields)
        self.records = 0
        self._file = path.open("w", encoding="utf-8", newline="")
        self._writer = csv.writer(self._file, delimiter="\t", lineterminator="\n")
        self._writer.writerow(self.fields)

    def write_columns(self, *columns: Sequence[str] | np.ndarray | pd.Series) -> None:
        """Write one row per position of the aligned ``columns`` (one per field)."""
        if len(columns) != len(self.fields):
            raise ValueError(f"expected {len(self.fields)} columns, got {len(columns)}")
        values = [present_values(column).tolist() for column in columns]
        self._writer.writerows(zip(*values))
        self.records += len(values[0])

    def close(self) -> None:
        self._file.close()


class GraphOutput:
    """The sinks and deduplication state for the graph kinds one source file produces."""

    def __init__(self, destination: Path, kinds: Sequence[str]) -> None:
        self.sinks = {kind: TsvSink(destination / GRAPH_FILES[kind][0], GRAPH_FILES[kind][1]) for kind in kinds}
        self.seen = {kind: KeySet() for kind in kinds if kind in DEDUPLICATED_KINDS}

    def write(self, kind: str, *columns: Sequence[str] | np.ndarray | pd.Series) -> None:
        self.sinks[kind].write_columns(*columns)

    def write_unique(self, kind: str, keys: np.ndarray, *columns: np.ndarray) -> None:
        """Write rows whose non-empty key was not written before (first occurrence wins)."""
        keys = np.asarray(keys, dtype=object)
        nonempty = np.flatnonzero(keys != "")
        # A C hash table drops in-chunk repeats before the (slower) digests.
        candidates = nonempty[~pd.Series(keys[nonempty], dtype=object).duplicated().to_numpy()]
        chosen = candidates[self.seen[kind].add(keys[candidates])]
        self.write(kind, *(np.asarray(column, dtype=object)[chosen] for column in columns))

    def close(self) -> None:
        for sink in self.sinks.values():
            sink.close()

    def summary(self) -> tuple[dict[str, int], dict[str, int]]:
        return (
            {kind: len(seen) for kind, seen in self.seen.items()},
            {GRAPH_FILES[kind][0]: sink.records for kind, sink in self.sinks.items()},
        )


def column(frame: pd.DataFrame, name: str) -> np.ndarray:
    return frame[name].to_numpy(dtype=object)


def derive_titles(frame: pd.DataFrame, out: GraphOutput, titles: KeySet, people: KeySet) -> None:
    # Like every other file, basics rows are kept by ID: a selected ID keeps all its rows.
    frame = frame.loc[titles.contains(frame["tconst"])]
    tconst, title_type = column(frame, "tconst"), present_values(frame["titleType"])
    out.write("title", tconst, title_type, frame["primaryTitle"], frame["originalTitle"], frame["isAdult"], frame["startYear"], frame["endYear"], frame["runtimeMinutes"])
    out.write_unique("title_type", title_type, title_type)
    typed = title_type != ""
    out.write("has_title_type", tconst[typed], title_type[typed])
    owners, genres = explode_list(tconst, frame["genres"])
    out.write_unique("genre", genres, genres)
    out.write("has_genre", owners, genres)


def derive_people(frame: pd.DataFrame, out: GraphOutput, titles: KeySet, people: KeySet) -> None:
    frame = frame.loc[people.contains(frame["nconst"])]
    nconst = column(frame, "nconst")
    out.write("person", nconst, frame["primaryName"], frame["birthYear"], frame["deathYear"])
    owners, professions = explode_list(nconst, frame["primaryProfession"])
    out.write_unique("profession", professions, professions)
    out.write("has_profession", owners, professions)
    owners, known = explode_list(nconst, frame["knownForTitles"])
    listed = titles.contains(known)
    out.write("known_for", owners[listed], known[listed])


def derive_principals(frame: pd.DataFrame, out: GraphOutput, titles: KeySet, people: KeySet) -> None:
    frame = frame.loc[titles.contains(frame["tconst"]) & people.contains(frame["nconst"])]
    tconst, nconst, category = column(frame, "tconst"), column(frame, "nconst"), present_values(frame["category"])
    out.write("credited_on", nconst, tconst, category, frame["ordering"], frame["job"])
    acting = pd.Series(category, dtype=object).isin(ACTING_CATEGORIES).to_numpy()
    out.write("acted_in", nconst[acting], tconst[acting], category[acting], column(frame, "ordering")[acting])
    positions, names = explode_characters(frame["characters"])
    out.write_unique("character", names, names)
    out.write("played", nconst[positions], names, tconst[positions])


def derive_crew(frame: pd.DataFrame, out: GraphOutput, titles: KeySet, people: KeySet) -> None:
    frame = frame.loc[titles.contains(frame["tconst"])]
    tconst = column(frame, "tconst")
    for field, kind in (("directors", "directed"), ("writers", "wrote")):
        title_ids, person_ids = explode_list(tconst, frame[field])
        credited = people.contains(person_ids)
        out.write(kind, person_ids[credited], title_ids[credited])


def derive_episodes(frame: pd.DataFrame, out: GraphOutput, titles: KeySet, people: KeySet) -> None:
    frame = frame.loc[titles.contains(frame["tconst"]) & titles.contains(frame["parentTconst"])]
    out.write("episode_of", frame["tconst"], frame["parentTconst"], frame["seasonNumber"], frame["episodeNumber"])


def derive_akas(frame: pd.DataFrame, out: GraphOutput, titles: KeySet, people: KeySet) -> None:
    frame = frame.loc[titles.contains(frame["titleId"])]
    title_id, ordering = column(frame, "titleId"), column(frame, "ordering")
    aka_id = (pd.Series(title_id, dtype=object) + ":" + pd.Series(ordering, dtype=object)).to_numpy(dtype=object)
    out.write_unique("alternative_title", aka_id, aka_id, title_id, ordering, column(frame, "title"), column(frame, "isOriginalTitle"))
    out.write("has_alternative_title", title_id, aka_id)
    for field, node_kind, relationship_kind in (("region", "region", "available_in_region"), ("language", "language", "in_language")):
        codes = present_values(frame[field])
        coded = codes != ""
        out.write_unique(node_kind, codes, codes)
        out.write(relationship_kind, aka_id[coded], codes[coded])


def derive_ratings(frame: pd.DataFrame, out: GraphOutput, titles: KeySet, people: KeySet) -> None:
    frame = frame.loc[titles.contains(frame["tconst"])]
    tconst = column(frame, "tconst")
    out.write_unique("rating", tconst, tconst, tconst, column(frame, "averageRating"), column(frame, "numVotes"))
    out.write("has_rating", tconst, tconst)


TITLE_COLUMNS = ("tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres")
PERSON_COLUMNS = ("nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles")
# Every source file: (derive, columns read, graph kinds written).  Each kind is
# written from exactly one file, so the files are independent once the title
# and person ID sets are known.  Listed longest first: the order parallel
# derivation starts them in.
SOURCES: dict[str, tuple[Callable[[pd.DataFrame, GraphOutput, KeySet, KeySet], None], tuple[str, ...], tuple[str, ...]]] = {
    "title.principals.tsv": (derive_principals, ("tconst", "ordering", "nconst", "category", "job", "characters"), ("credited_on", "acted_in", "character", "played")),
    "title.akas.tsv": (derive_akas, ("titleId", "ordering", "title", "region", "language", "isOriginalTitle"), ("alternative_title", "region", "language", "has_alternative_title", "available_in_region", "in_language")),
    "name.basics.tsv": (derive_people, PERSON_COLUMNS, ("person", "profession", "has_profession", "known_for")),
    "title.basics.tsv": (derive_titles, TITLE_COLUMNS, ("title", "title_type", "genre", "has_title_type", "has_genre")),
    "title.crew.tsv": (derive_crew, ("tconst", "directors", "writers"), ("directed", "wrote")),
    "title.episode.tsv": (derive_episodes, ("tconst", "parentTconst", "seasonNumber", "episodeNumber"), ("episode_of",)),
    "title.ratings.tsv": (derive_ratings, ("tconst", "averageRating", "numVotes"), ("rating", "has_rating")),
}

Selector = Callable[[pd.DataFrame], "pd.Series | np.ndarray"]
Summary = tuple[dict[str, int], dict[str, int]]


def collect_ids(path: Path, key: str, columns: Sequence[str], select: Selector, *, verbose: bool) -> KeySet:
    """The IDs of the selected rows of a ``title.basics``/``name.basics`` file."""
    ids = KeySet()
    for frame in tsv_chunks(path, list(columns), f"selected IDs from {path.name}", verbose=verbose):
        ids.add(frame.loc[np.asarray(select(frame), dtype=bool), key])
    ids.freeze()
    return ids


def derive_source(input_dir: Path, destination: Path, name: str, titles: KeySet, people: KeySet, *, verbose: bool) -> Summary:
    """Stream one source file into the graph files it produces."""
    derive, columns, kinds = SOURCES[name]
    out = GraphOutput(destination, kinds)
    try:
        for frame in tsv_chunks(input_dir / name, list(columns), f"graph from {name}", verbose=verbose):
            derive(frame, out, titles, people)
    finally:
        out.close()
    return out.summary()


def _run_in_child(connection, task: Callable[[], Summary]) -> None:
    try:
        connection.send(("ok", task()))
    except BaseException:  # report every failure to the parent, which fails closed
        connection.send(("error", traceback.format_exc()))
    finally:
        connection.close()


def _start_child(context, name: str, task: Callable[[], object]) -> tuple[object, multiprocessing.Process]:
    """Fork a process running ``task``; it inherits the parent's ID sets without copying them."""
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_run_in_child, args=(sender, task), name=f"graph-{name}")
    process.start()
    sender.close()
    logger.info("Started %s in process %s", name, process.pid)
    return receiver, process


def _child_result(name: str, receiver, process: multiprocessing.Process) -> object:
    try:
        status, payload = receiver.recv()
    except EOFError:
        status, payload = "error", "process exited without a result"
    process.join()
    if status != "ok":
        raise RuntimeError(f"{name} failed (exit code {process.exitcode}):\n{payload}")
    logger.info("Finished %s", name)
    return payload


def _derive_in_processes(tasks: dict[str, Callable[[], Summary]], jobs: int) -> Iterable[Summary]:
    """Run ``tasks`` in at most ``jobs`` forked processes, yielding results as they finish."""
    context = multiprocessing.get_context("fork")
    pending, running = list(tasks), {}
    try:
        while pending or running:
            while pending and len(running) < jobs:
                name = pending.pop(0)
                receiver, process = _start_child(context, f"graph from {name}", tasks[name])
                running[receiver] = (name, process)
            for receiver in wait_for_connections(list(running)):
                name, process = running.pop(receiver)
                yield _child_result(f"graph from {name}", receiver, process)
    finally:
        for _, process in running.values():
            process.terminate()
            process.join()


def derive_graph(
    input_dir: Path,
    destination: Path,
    select_titles: Selector,
    select_people: Selector,
    *,
    verbose: bool = False,
    jobs: int = 1,
) -> tuple[dict[str, int], dict[str, int]]:
    """Derive graph node and relationship TSVs for the selected titles and people.

    ``select_titles``/``select_people`` return a row mask for a
    ``title.basics``/``name.basics`` chunk.  Only selected IDs present in
    those files become nodes, and only nodes may be relationship endpoints.

    A first pass collects the two ID sets; then every source file is derived
    independently.  With ``jobs > 1`` they run concurrently in forked
    processes, each writing only its own output files, so the result is
    byte-identical to ``jobs=1``.  Returns ``(selected counts, records
    written per graph file)``.
    """
    started = time.monotonic()

    def collect_people() -> KeySet:
        return collect_ids(input_dir / "name.basics.tsv", "nconst", PERSON_COLUMNS, select_people, verbose=verbose)

    if jobs == 1:
        titles, people = collect_ids(input_dir / "title.basics.tsv", "tconst", TITLE_COLUMNS, select_titles, verbose=verbose), collect_people()
    else:  # collect the person IDs in a child while this process collects the title IDs
        receiver, process = _start_child(multiprocessing.get_context("fork"), "person ID selection", collect_people)
        try:
            titles = collect_ids(input_dir / "title.basics.tsv", "tconst", TITLE_COLUMNS, select_titles, verbose=verbose)
        except BaseException:
            process.terminate()
            process.join()
            raise
        people = _child_result("person ID selection", receiver, process)
    logger.info("Selected %s title ID(s) and %s person ID(s) in %.1fs", f"{len(titles):,}", f"{len(people):,}", time.monotonic() - started)
    tasks = {name: (lambda name=name: derive_source(input_dir, destination, name, titles, people, verbose=verbose)) for name in SOURCES}
    summaries = (task() for task in tasks.values()) if jobs == 1 else _derive_in_processes(tasks, jobs)
    seen: dict[str, int] = {}
    records: dict[str, int] = {}
    for kind_counts, file_records in summaries:
        seen.update(kind_counts)
        records.update(file_records)
    counts = {"titles": len(titles), "people": len(people), **{kind: seen[kind] for kind in DEDUPLICATED_KINDS}}
    logger.info("Derived graph TSVs in %s in %.1fs", destination, time.monotonic() - started)
    return counts, {relative: records[relative] for relative, _ in GRAPH_FILES.values()}


def graph_subset(input_dir: Path, destination: Path, titles: set[str], people: set[str], *, verbose: bool = False, jobs: int = 1) -> dict[str, int]:
    """Create graph-oriented node and relationship TSVs for the selected IDs."""
    # Credits can refer to IDs absent from the corresponding IMDb basics file.
    # Only IDs that will actually become nodes may be used as edge endpoints.
    counts, _ = derive_graph(
        input_dir, destination,
        lambda frame: frame["tconst"].isin(titles), lambda frame: frame["nconst"].isin(people),
        verbose=verbose, jobs=jobs,
    )
    return counts


def eligible_titles(frame: pd.DataFrame, include_adult: bool) -> pd.Series:
    """Rows of a ``title.basics`` chunk that may be selected: typed, named, and non-adult unless allowed."""
    eligible = frame["titleType"].ne(MISSING) & frame["titleType"].ne("") & frame["primaryTitle"].ne(MISSING) & frame["primaryTitle"].ne("")
    if not include_adult:
        eligible &= frame["isAdult"].eq("0")
    return eligible


def select_seed_titles(path: Path, count: int, include_adult: bool, *, offset: int = 0, verbose: bool) -> set[str]:
    selected: set[str] = set()
    skip_remaining = offset
    for frame in tsv_chunks(path, ["tconst", "titleType", "primaryTitle", "isAdult"], "seed-title selection", verbose=verbose):
        eligible_ids = frame.loc[eligible_titles(frame, include_adult), "tconst"]
        if skip_remaining:
            skipped = min(skip_remaining, len(eligible_ids))
            eligible_ids = eligible_ids.iloc[skipped:]
            skip_remaining -= skipped
        for title_id in eligible_ids:
            selected.add(title_id)
            if len(selected) == count:
                return selected
    raise ValueError(f"only found {len(selected)} eligible titles after offset {offset}; requested {count}")


def export_all(input_dir: Path, output: Path, include_adult: bool, *, verbose: bool, jobs: int) -> dict:
    """Convert the entire dataset: every eligible title and every person.

    No ``source/`` copies are written: they would duplicate the extracted
    TSVs, and the graph files are derived from those directly.
    """
    counts, records = derive_graph(
        input_dir, output / "graph",
        lambda frame: eligible_titles(frame, include_adult), lambda frame: np.ones(len(frame), dtype=bool),
        verbose=verbose, jobs=jobs,
    )
    return {"mode": "all", "include_adult": include_adult, "source_copies": False, "selected": counts, "records": records}


def main() -> int:
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input-dir", type=Path, default=Path("examples/imdb/data/extracted"))
    parser.add_argument("--output-dir", type=Path, default=Path("examples/imdb/data/subsets"))
    parser.add_argument("--titles", type=int, help="number of seed titles (required unless --all)")
    parser.add_argument("--offset", type=int, default=0, help="eligible seed titles to skip before selecting --titles (default: 0)")
    parser.add_argument("--depth", type=int, default=2, help="Title↔Person credit hops from the seed titles")
    parser.add_argument("--max-titles", type=int, help="final title budget (default: ten times --titles)")
    parser.add_argument("--max-people", type=int, help="final person budget (default: fifty times --titles)")
    parser.add_argument("--all", action="store_true", help="convert every eligible title and every person to <output-dir>/all/ (all-with-adult/ with --include-adult); writes no source/ copies")
    parser.add_argument("--include-adult", action="store_true", help="allow adult seed titles (with --all: include adult titles)")
    parser.add_argument("--jobs", type=int, help="processes deriving independent source files concurrently (default: 4 with --all, else 1)")
    parser.add_argument("--verbose", action="store_true", help="log million-row progress and phase durations")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    jobs = args.jobs if args.jobs is not None else (4 if args.all else 1)
    if jobs < 1:
        parser.error("--jobs must be positive")
    missing = [name for name in REQUIRED_FILES if not (args.input_dir / name).is_file()]
    if args.all:
        if args.titles is not None or args.offset or args.max_titles is not None or args.max_people is not None:
            parser.error("--all selects every title and person; it cannot be combined with --titles, --offset, --max-titles or --max-people")
        if missing:
            parser.error(f"missing extracted IMDb files: {', '.join(missing)}")
        output = args.output_dir / ("all-with-adult" if args.include_adult else "all")
        logger.info("Converting the entire IMDb dataset from %s to %s (adult titles %s, %s job(s))", args.input_dir, output, "included" if args.include_adult else "excluded", jobs)
        manifest = export_all(args.input_dir, output, args.include_adult, verbose=args.verbose, jobs=jobs)
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        logger.info("Wrote full export to %s", output)
        logger.info("Selected graph records: %s", json.dumps(manifest["selected"], sort_keys=True))
        logger.info("IMDb full export completed in %.1fs", time.monotonic() - started)
        return 0

    if args.titles is None:
        parser.error("--titles is required unless --all is given")
    logger.info("Building IMDb subset from %s to %s with %s seed titles, offset %s, and depth %s", args.input_dir, args.output_dir, args.titles, args.offset, args.depth)
    if args.titles < 1 or args.offset < 0 or args.depth < 0:
        parser.error("--titles must be positive; --offset and --depth cannot be negative")
    if missing:
        parser.error(f"missing extracted IMDb files: {', '.join(missing)}")

    title_limit = args.max_titles or args.titles * 10
    person_limit = args.max_people or args.titles * 50
    if title_limit < args.titles or person_limit < 1:
        parser.error("budgets must be positive and --max-titles cannot be below --titles")

    logger.info("Using title budget %s and person budget %s", title_limit, person_limit)
    titles = select_seed_titles(args.input_dir / "title.basics.tsv", args.titles, args.include_adult, offset=args.offset, verbose=args.verbose)
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

    subset_name = f"titles-{args.titles}-depth-{args.depth}" if args.offset == 0 else f"titles-{args.titles}-offset-{args.offset}-depth-{args.depth}"
    output = args.output_dir / subset_name
    logger.info("Writing source subset with %s title(s) and %s person(s) to %s", len(titles), len(people), output)
    source_subset(args.input_dir, output / "source", titles, people, verbose=args.verbose)
    # Graph derivation now scans only the tiny filtered source copies, rather
    # than re-reading the full 9+ GB extracted dataset a second time.
    logger.info("Deriving graph node and relationship TSVs from filtered source copies")
    counts, records = derive_graph(
        output / "source", output / "graph",
        lambda frame: frame["tconst"].isin(titles), lambda frame: frame["nconst"].isin(people),
        verbose=args.verbose, jobs=jobs,
    )
    manifest = {
        "seed_titles": args.titles,
        "seed_offset": args.offset,
        "depth": args.depth,
        "title_budget": title_limit,
        "person_budget": person_limit,
        "selected": counts,
        "records": records,
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    logger.info("Wrote subset to %s", output)
    logger.info("Selected graph records: %s", json.dumps(manifest["selected"], sort_keys=True))
    logger.info("IMDb subset build completed in %.1fs", time.monotonic() - started)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
