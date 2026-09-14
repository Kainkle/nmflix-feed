#!/usr/bin/env python3
"""
build.py: the NMFlix catalogue feed. See docs/NMFLIX_ROWS_PLAN.md.

    python tools/build.py --out <dir>                # full build
    python tools/build.py --out <dir> --limit 150    # smoke run, first 150 titles
    python tools/build.py --out <dir> --cache <dir>  # keep per-title responses between runs

Writes three files into --out:

    catalog.json   every title the app can show, with its art already chosen
    rows.json      candidate rows the box ranks and personalises; it never discovers on its own
    report.json    what was built, what was missing, how big it is

The key comes from the TMDB_API_KEY environment variable (GitHub Actions) or from local.properties
(a local run). It is never printed and never written into the output.

Art rules (the plan, section "Art rules the feed enforces"):
  backdrop  the best textless backdrop (iso_639_1 null), 16:9 within tolerance, at least 1280 wide,
            ranked by vote average with a vote-count floor, ties to the wider upload
  logo      the top-voted English PNG logo, preferring wide wordmarks over tall crests
  poster    only a fallback for a title with no backdrop
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

API = "https://api.themoviedb.org/3"
IMAGE = "https://image.tmdb.org/t/p/"
IMDB_RATINGS = "https://datasets.imdbws.com/title.ratings.tsv.gz"

# Image rungs the app draws. The card is w780 (CardBandW 334dp at 2x is 668px), the detail backdrop
# w1280 fills the frame, the logo w500, the poster fallback w342.
SIZE_CARD = "w780"
SIZE_BACKDROP = "w1280"
SIZE_LOGO = "w500"
SIZE_POSTER = "w342"

# Backdrop selection.
BACKDROP_MIN_W = 1280
BACKDROP_ASPECT = 16 / 9
BACKDROP_ASPECT_TOL = 0.04
BACKDROP_VOTE_FLOOR = 2          # under this many votes the average means nothing; fall back to width
LOGO_WIDE_RATIO = 4.0            # width / height at or above this is a wordmark; below it a crest
OVERVIEW_CHARS = 300             # the row detail shows three lines; the detail page fetches the rest live
KEYWORDS_MAX = 8
RECS_MAX = 12
CAST_MAX = 10          # billed cast with character names: the search index matches "beast boy" and "kevin hart"
DIRECTORS_MAX = 2
POPULAR_PAGES = {"movie": 150, "tv": 100}   # the popular name index: 20 a page, so 3,000 films and 2,000 series
IMDB_VOTE_FLOOR = 1000
MOVIE_MIN_RUNTIME = 60           # a short film in a feature row reads as a mistake (Thriller, 1983, 13 minutes)

# Quality prior, the same numbers RowAssembler.qualityPrior uses on the box, so a score here and there agree.
PRIOR_WEIGHT = 300.0
GLOBAL_MEAN = 6.4

RATE_PER_S = 20                  # TMDB allows about 50/s; this leaves room for every box's live calls
WORKERS = 8
RETRIES = 4

THEATRICAL_WINDOW_DAYS = 15
NEW_WEEK_DAYS = 7


# ------------------------------------------------------------------ the universe and the candidate rows
#
# Every row kind the planner may use, with the query that generates its members. `kind` is the archetype
# the box's RowAssembler already knows. Labels with {genre} are filled per genre. `pages` is how deep to
# read; two pages of twenty is enough for a row that shows ten.
def cohorts(today: date) -> list[dict]:
    since_theatre = (today - timedelta(days=THEATRICAL_WINDOW_DAYS)).isoformat()
    since_week = (today - timedelta(days=NEW_WEEK_DAYS)).isoformat()
    rows: list[dict] = [
        # Anchors
        {"id": "trending-day", "kind": "TRENDING", "label": "Trending Now", "path": "/trending/all/day", "pages": 2, "type": "mixed"},
        {"id": "trending-week", "kind": "TRENDING", "label": "Trending This Week", "path": "/trending/all/week", "pages": 2, "type": "mixed"},
        {"id": "new-theatres", "kind": "TRENDING", "label": "New in Theatres", "path": "/discover/movie", "pages": 2, "type": "movie",
         "query": {"primary_release_date.gte": since_theatre, "with_release_type": "2|3", "sort_by": "popularity.desc"}},
        {"id": "new-week", "kind": "TRENDING", "label": "New This Week", "path": "/discover/movie", "pages": 1, "type": "movie",
         "query": {"primary_release_date.gte": since_week, "sort_by": "popularity.desc", "vote_count.gte": 20}},
        {"id": "airing-week", "kind": "TRENDING", "label": "Airing This Week", "path": "/tv/on_the_air", "pages": 2, "type": "tv"},
        # Popular / top rated, both types
        {"id": "popular-movie", "kind": "POPULAR", "label": "Popular Movies", "path": "/movie/popular", "pages": 3, "type": "movie"},
        {"id": "popular-tv", "kind": "POPULAR", "label": "Popular Series", "path": "/tv/popular", "pages": 3, "type": "tv"},
        {"id": "top-movie", "kind": "POPULAR", "label": "Top Rated Movies", "path": "/movie/top_rated", "pages": 3, "type": "movie"},
        {"id": "top-tv", "kind": "POPULAR", "label": "Top Rated Series", "path": "/tv/top_rated", "pages": 3, "type": "tv"},
    ]
    # Broad genres, one row each, per type. Ids are TMDB's own.
    movie_genres = {28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime", 99: "Documentary", 18: "Drama",
                    10751: "Family", 14: "Fantasy", 27: "Horror", 9648: "Mystery", 10749: "Romance", 878: "Science Fiction",
                    53: "Thriller", 10752: "War", 37: "Western"}
    tv_genres = {10759: "Action & Adventure", 16: "Animation", 35: "Comedy", 80: "Crime", 99: "Documentary", 18: "Drama",
                 10751: "Family", 9648: "Mystery", 10765: "Sci-Fi & Fantasy", 10768: "War & Politics", 37: "Western", 10764: "Reality"}
    for gid, name in movie_genres.items():
        rows.append({"id": f"genre-movie-{gid}", "kind": "GENRE", "label": f"{name} Movies", "path": "/discover/movie", "pages": 2,
                     "type": "movie", "genre": gid,
                     "query": {"with_genres": str(gid), "sort_by": "popularity.desc", "vote_count.gte": 200}})
    for gid, name in tv_genres.items():
        rows.append({"id": f"genre-tv-{gid}", "kind": "GENRE", "label": f"{name} Series", "path": "/discover/tv", "pages": 2,
                     "type": "tv", "genre": gid,
                     "query": {"with_genres": str(gid), "sort_by": "popularity.desc", "vote_count.gte": 100}})
    # Micro-genres: genre x era, the signature rows. Only the pairings a viewer would name.
    eras = [("80s", "1980-01-01", "1989-12-31"), ("90s", "1990-01-01", "1999-12-31"), ("2000s", "2000-01-01", "2009-12-31")]
    micro = [(80, "Crime"), (878, "Sci-Fi"), (27, "Horror"), (35, "Comedy"), (28, "Action"), (53, "Thriller"), (16, "Animated")]
    for era, lo, hi in eras:
        for gid, name in micro:
            rows.append({"id": f"micro-movie-{gid}-{era}", "kind": "MICRO_GENRE", "label": f"{name} from the {era}", "path": "/discover/movie",
                         "pages": 1, "type": "movie", "genre": gid, "era": era,
                         "query": {"with_genres": str(gid), "primary_release_date.gte": lo, "primary_release_date.lte": hi,
                                   "sort_by": "vote_average.desc", "vote_count.gte": 400}})
    # Hidden gems: well rated, not popular.
    rows.append({"id": "gems-movie", "kind": "MICRO_GENRE", "label": "Hidden Gems", "path": "/discover/movie", "pages": 2, "type": "movie",
                 "query": {"sort_by": "vote_average.desc", "vote_count.gte": 300, "vote_count.lte": 3000, "primary_release_date.gte": "2010-01-01"}})
    rows.append({"id": "gems-tv", "kind": "MICRO_GENRE", "label": "Hidden Gem Series", "path": "/discover/tv", "pages": 2, "type": "tv",
                 "query": {"sort_by": "vote_average.desc", "vote_count.gte": 150, "vote_count.lte": 1500, "first_air_date.gte": "2010-01-01"}})
    return rows


# ------------------------------------------------------------------ TMDB client
class Tmdb:
    def __init__(self, key: str, cache: Path | None):
        self.key = key
        self.cache = cache
        self._lock = threading.Lock()
        self._next = 0.0
        self.calls = 0
        self.errors = 0

    def _pace(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next - now
            self._next = max(now, self._next) + 1.0 / RATE_PER_S
        if wait > 0:
            time.sleep(wait)

    def get(self, path: str, **query) -> dict | None:
        query["api_key"] = self.key
        url = f"{API}{path}?{urllib.parse.urlencode(query)}"
        for attempt in range(RETRIES):
            self._pace()
            try:
                with urllib.request.urlopen(url, timeout=30) as r:
                    self.calls += 1
                    return json.load(r)
            except urllib.error.HTTPError as e:
                self.calls += 1
                if e.code == 404:
                    return None
                if e.code in (429, 500, 502, 503, 504):
                    time.sleep(1.5 * (attempt + 1))
                    continue
                self.errors += 1
                # The URL carries the key: log the path only.
                print(f"  http {e.code} on {path}", file=sys.stderr)
                return None
            except (urllib.error.URLError, TimeoutError, OSError):
                time.sleep(1.5 * (attempt + 1))
        self.errors += 1
        print(f"  gave up on {path}", file=sys.stderr)
        return None

    def title(self, kind: str, tid: int) -> dict | None:
        """One call per title: details, images (en + textless), external ids, keywords, recommendations,
        certifications and credits, exactly as the box's own detailsFor does it."""
        if self.cache:
            p = self.cache / f"{kind}-{tid}.json"
            if p.exists():
                return json.loads(p.read_text(encoding="utf-8"))
        extra = "release_dates" if kind == "movie" else "content_ratings"
        j = self.get(f"/{kind}/{tid}", append_to_response=f"images,external_ids,keywords,recommendations,credits,{extra}",
                     include_image_language="en,null")
        if j is not None and self.cache:
            self.cache.mkdir(parents=True, exist_ok=True)
            (self.cache / f"{kind}-{tid}.json").write_text(json.dumps(j), encoding="utf-8")
        return j


# ------------------------------------------------------------------ art rules
def pick_backdrop(images: dict, fallback: str | None) -> str | None:
    """Textless, 16:9, wide and well voted first; then any textless; then an English one; then whatever
    the list payload carried. A title is never left without a picture for want of a perfect one."""
    tiers: list[list[tuple]] = [[], [], []]
    for b in images.get("backdrops", []):
        w, h = b.get("width", 0), b.get("height", 0)
        if h == 0:
            continue
        votes = b.get("vote_count", 0)
        avg = b.get("vote_average", 0.0) if votes >= BACKDROP_VOTE_FLOOR else 0.0
        cand = (avg, votes, w, b["file_path"])
        if b.get("iso_639_1") is None:
            if w >= BACKDROP_MIN_W and abs(w / h - BACKDROP_ASPECT) <= BACKDROP_ASPECT_TOL:
                tiers[0].append(cand)
            else:
                tiers[1].append(cand)
        elif b.get("iso_639_1") == "en":
            tiers[2].append(cand)
    for tier in tiers:
        if tier:
            tier.sort(reverse=True)
            return tier[0][3]
    return fallback


def pick_logo(images: dict) -> str | None:
    cands = []
    for l in images.get("logos", []):
        path = l.get("file_path", "")
        if not path.endswith(".png") or l.get("iso_639_1") not in ("en", None):
            continue
        w, h = l.get("width", 0), l.get("height", 1)
        wide = 1 if h and w / h >= LOGO_WIDE_RATIO else 0
        cands.append((wide, l.get("vote_average", 0.0), l.get("vote_count", 0), path))
    if not cands:
        return None
    cands.sort(reverse=True)
    return cands[0][3]


def certification(kind: str, j: dict) -> str:
    if kind == "movie":
        for entry in j.get("release_dates", {}).get("results", []):
            if entry.get("iso_3166_1") == "US":
                for rd in entry.get("release_dates", []):
                    c = rd.get("certification", "").strip()
                    if c:
                        return c
    else:
        for entry in j.get("content_ratings", {}).get("results", []):
            if entry.get("iso_3166_1") == "US" and entry.get("rating"):
                return entry["rating"]
    return ""


def quality_prior(avg: float, votes: int) -> float:
    v = float(votes)
    return (v / (v + PRIOR_WEIGHT)) * avg + (PRIOR_WEIGHT / (v + PRIOR_WEIGHT)) * GLOBAL_MEAN


# ------------------------------------------------------------------ IMDb ratings (same dataset the box carries)
def imdb_ratings(cache: Path | None) -> dict[str, float]:
    """tconst -> rating, for titles with at least IMDB_VOTE_FLOOR votes. Licence: personal, non-commercial;
    see tools/make_imdb_index.py. Skipped, with an empty map, when the download fails."""
    src = None
    if cache:
        cache.mkdir(parents=True, exist_ok=True)
        src = cache / "title.ratings.tsv.gz"
        if not src.exists() or time.time() - src.stat().st_mtime > 86400:
            try:
                urllib.request.urlretrieve(IMDB_RATINGS, src)
            except Exception as e:  # noqa: BLE001
                print(f"  imdb ratings download failed: {type(e).__name__}", file=sys.stderr)
                return {}
    else:
        try:
            src = Path("title.ratings.tsv.gz")
            urllib.request.urlretrieve(IMDB_RATINGS, src)
        except Exception as e:  # noqa: BLE001
            print(f"  imdb ratings download failed: {type(e).__name__}", file=sys.stderr)
            return {}
    out: dict[str, float] = {}
    with gzip.open(src, "rt", encoding="utf-8") as f:
        next(f)
        for line in f:
            tconst, rating, votes = line.rstrip("\n").split("\t")
            if int(votes) >= IMDB_VOTE_FLOOR:
                out[tconst] = float(rating)
    return out


# ------------------------------------------------------------------ build
def popular_index(tmdb: "Tmdb") -> list[dict]:
    """Discover, by popularity, with at least 50 votes: name, year, kind, backdrop, popularity, vote, alternate
    name and genre ids per title. About 650 KB raw, 150 KB gzipped."""
    jobs = [(kind, n) for kind, count in POPULAR_PAGES.items() for n in range(1, count + 1)]

    def page(job: tuple[str, int]) -> list[dict]:
        kind, n = job
        j = tmdb.get(f"/discover/{kind}", sort_by="popularity.desc", page=n, include_adult="false", language="en-US", **{"vote_count.gte": 50})
        if not j:
            return []
        return [{"id": f"{kind}-{it['id']}", "t": kind, "n": it.get("title") or it.get("name") or "",
                 "y": (it.get("release_date") or it.get("first_air_date") or "")[:4], "bd": it.get("backdrop_path"),
                 "pop": round(it.get("popularity", 0), 1), "va": round(it.get("vote_average", 0), 1),
                 "alt": it.get("original_title") or it.get("original_name") or "", "gi": it.get("genre_ids", [])}
                for it in j.get("results", []) if it.get("backdrop_path") and (it.get("title") or it.get("name"))]

    out: list[dict] = []
    seen: set[str] = set()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for lst in pool.map(page, jobs):
            for it in lst:
                if it["id"] not in seen:
                    seen.add(it["id"])
                    out.append(it)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--cache", default=None, help="directory for per-title responses and the IMDb dataset")
    ap.add_argument("--limit", type=int, default=0, help="only the first N titles of the universe (smoke run)")
    args = ap.parse_args()

    key = os.environ.get("TMDB_API_KEY")
    if not key:
        lp = Path("local.properties")
        if lp.exists():
            for line in lp.read_text(encoding="utf-8").splitlines():
                m = re.match(r"\s*(tmdb[._]?api[._]?key|TMDB_API_KEY|tmdbKey|tmdb\.key)\s*=\s*(.+)", line, re.I)
                if m:
                    key = m.group(2).strip().strip('"')
                    break
    if not key:
        print("no TMDB key: set TMDB_API_KEY or put it in local.properties", file=sys.stderr)
        return 2

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    cache = Path(args.cache) if args.cache else None
    tmdb = Tmdb(key, cache)
    today = date.today()
    started = time.monotonic()

    # 1. Cohorts -> universe. Each cohort remembers its member ids in the order TMDB returned them.
    rows = cohorts(today)
    universe: dict[str, tuple[str, int]] = {}
    for row in rows:
        members: list[str] = []
        for page in range(1, row["pages"] + 1):
            j = tmdb.get(row["path"], page=page, **row.get("query", {}))
            if not j:
                break
            for item in j.get("results", []):
                mt = item.get("media_type") or row["type"]
                if mt not in ("movie", "tv"):
                    continue
                if not item.get("backdrop_path") and not item.get("poster_path"):
                    continue
                k = f"{mt}-{item['id']}"
                universe.setdefault(k, (mt, item["id"]))
                if k not in members:
                    members.append(k)
        row["members"] = members
    keys = list(universe)
    if args.limit:
        keys = keys[: args.limit]
    print(f"universe {len(universe)} titles from {len(rows)} cohorts, building {len(keys)}")

    # 2. One call per title, paced, in parallel.
    ratings = imdb_ratings(cache)
    print(f"imdb ratings loaded: {len(ratings)}")
    catalog: dict[str, dict] = {}
    missing_backdrop = 0
    missing_logo = 0

    def build(k: str) -> tuple[str, dict | None]:
        kind, tid = universe[k]
        j = tmdb.title(kind, tid)
        if not j:
            return k, None
        images = j.get("images", {})
        backdrop = pick_backdrop(images, j.get("backdrop_path"))
        logo = pick_logo(images)
        kw = j.get("keywords", {})
        kws = kw.get("keywords") or kw.get("results") or []
        recs = [f"{r.get('media_type') or kind}-{r['id']}" for r in j.get("recommendations", {}).get("results", [])]
        imdb_id = j.get("imdb_id") or j.get("external_ids", {}).get("imdb_id")
        release = j.get("release_date") or j.get("first_air_date") or ""
        runtime = j.get("runtime") or (j.get("episode_run_time") or [None])[0]
        entry = {
            "t": kind,
            "n": j.get("title") or j.get("name") or "",
            "y": release[:4],
            "d": release,
            "g": [g["name"] for g in j.get("genres", [])],
            "k": [x["name"] for x in kws[:KEYWORDS_MAX]],
            "va": round(j.get("vote_average", 0.0), 2),
            "vc": j.get("vote_count", 0),
            "q": round(quality_prior(j.get("vote_average", 0.0), j.get("vote_count", 0)), 3),
            "pop": round(j.get("popularity", 0.0), 1),
            "cert": certification(kind, j),
            "rt": runtime,
            "seasons": j.get("number_of_seasons"),
            "col": (j.get("belongs_to_collection") or {}).get("id"),
            # Names and character names, not ids: the box's search matches both without a request.
            "cast": [{"n": c.get("name", ""), "c": c.get("character", "")} for c in j.get("credits", {}).get("cast", [])[:CAST_MAX]],
            "dir": ([c.get("name", "") for c in j.get("credits", {}).get("crew", []) if c.get("job") == "Director"]
                    + [c.get("name", "") for c in j.get("created_by", [])])[:DIRECTORS_MAX],
            "imdb": imdb_id,
            "ir": ratings.get(imdb_id) if imdb_id else None,
            "o": (j.get("overview") or "")[:OVERVIEW_CHARS],
            "bd": backdrop,
            "lg": logo,
            "ps": j.get("poster_path"),
            "rec": recs[:RECS_MAX],
        }
        return k, entry

    dropped_short = 0
    dropped_blank = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for i, (k, entry) in enumerate(pool.map(build, keys), 1):
            if entry is None:
                continue
            # Not shown at all: a short, or a title with nothing to draw.
            if entry["t"] == "movie" and entry["rt"] is not None and entry["rt"] < MOVIE_MIN_RUNTIME:
                dropped_short += 1
                continue
            if entry["bd"] is None and entry["ps"] is None:
                dropped_blank += 1
                continue
            if entry["bd"] is None:
                missing_backdrop += 1
            if entry["lg"] is None:
                missing_logo += 1
            catalog[k] = entry
            if i % 100 == 0:
                print(f"  {i}/{len(keys)} titles, {tmdb.calls} calls, {time.monotonic() - started:.0f}s")

    # 3. Candidate rows: members that made it into the catalogue, in TMDB's order; the box re-ranks.
    # Recommendations are trimmed to the universe so a "Because you watched" row never needs a call.
    for entry in catalog.values():
        entry["rec"] = [r for r in entry["rec"] if r in catalog]
    candidates = [
        {"id": r["id"], "kind": r["kind"], "label": r["label"], "type": r["type"],
         **({"genre": r["genre"]} if "genre" in r else {}), **({"era": r["era"]} if "era" in r else {}),
         "members": [m for m in r["members"] if m in catalog]}
        for r in rows
    ]
    candidates = [c for c in candidates if len(c["members"]) >= 6]

    # 3b. The popular name index: the most popular titles anywhere, names only, so the search's instant layer
    # answers "joh" with John Wick before any request, whether or not the rows carry it.
    popular = popular_index(tmdb)
    print(f"popular index: {len(popular)} titles")

    # 4. Write. Compact JSON; the box reads it with JsonReader.
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    catalog_doc = {"generated_utc": generated, "image_base": IMAGE, "sizes": {"card": SIZE_CARD, "backdrop": SIZE_BACKDROP,
                   "logo": SIZE_LOGO, "poster": SIZE_POSTER}, "titles": catalog}
    rows_doc = {"generated_utc": generated, "rows": candidates}
    popular_doc = {"generated_utc": generated, "titles": popular}
    (out / "flix_catalog.json").write_text(json.dumps(catalog_doc, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    (out / "flix_rows.json").write_text(json.dumps(rows_doc, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    (out / "flix_popular.json").write_text(json.dumps(popular_doc, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    raw = (out / "flix_catalog.json").stat().st_size
    gz = len(gzip.compress((out / "flix_catalog.json").read_bytes()))
    report = {
        "generated_utc": generated,
        "universe": len(universe),
        "built": len(catalog),
        "cohorts": len(rows),
        "candidate_rows": len(candidates),
        "missing_backdrop": missing_backdrop,
        "dropped_short": dropped_short,
        "dropped_blank": dropped_blank,
        "missing_logo": missing_logo,
        "imdb_rated": sum(1 for e in catalog.values() if e["ir"] is not None),
        "tmdb_calls": tmdb.calls,
        "tmdb_errors": tmdb.errors,
        "catalog_bytes": raw,
        "catalog_gzip_bytes": gz,
        "seconds": round(time.monotonic() - started),
        "thin_rows": [c["id"] for c in candidates if len(c["members"]) < 10],
    }
    (out / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
