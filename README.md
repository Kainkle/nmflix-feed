# nmflix-feed

The NMFlix catalogue: every title the app can show, with its art already chosen, plus the candidate
rows the box ranks and personalises. Built nightly by `tools/build.py` from TMDB, committed into
`docs/`, served from raw.githubusercontent.

- `docs/flix_catalog.json`: the shelves, identical for every box. Nothing about a viewer is in it.
- `docs/flix_rows.json`: candidate rows (each cohort's members). The box never discovers on its own.
- `docs/report.json`: what was built, what was missing, how big it is.

The TMDB key is the `TMDB_API_KEY` Actions secret. It is never printed or written into the output.
The IMDb ratings join uses IMDb's non-commercial dataset; see the licence note in the NMLauncher repo's
`tools/make_imdb_index.py`.

Local run: `TMDB_API_KEY=... python tools/build.py --out docs --cache .cache`
