# Repository Guidelines

## Project Structure & Module Organization
- Source lives at `polygon_downloader.py`; it exposes the CLI entry point and the `PolygonFuturesDownloader` class.
- Dependencies are tracked in `requirements.txt`. Generated CSV artifacts (`<ticker>_<date>.csv`) stay out of version control in a dedicated `data/` folder.
- Create new modules under the repo root, and mirror them with tests inside a future `tests/` package.

## Build, Test, and Development Commands
- `python3 -m venv .venv && source .venv/bin/activate` creates an isolated environment (preferred for local work).
- `pip install -r requirements.txt` installs runtime dependencies; rerun after dependency updates.
- `python3 polygon_downloader.py ESZ5 2025-08-22 --api-key "$POLYGON_API_KEY" --max-pages 1` exercises the downloader against live data without exhausting quotas.
- `python3 polygon_downloader.py --help` prints CLI options and examples; update this output whenever flags change.

## Coding Style & Naming Conventions
- Target Python 3.10+. Follow PEP 8 with 4-space indentation and docstrings on public functions/classes.
- Keep functions small, pure where possible, and typed (`List[Dict[str, Any]]`, `Optional[int]`) to match existing patterns.
- Use verbs for command functions (`download_trades`), nouns for data containers, and snake_case for variables and files.

## Testing Guidelines
- Add a `tests/` directory using `pytest`; prefer descriptive filenames such as `test_download_trades.py`.
- Mock Polygon API calls (`responses` or `requests_mock`) to cover pagination, throttling, and error responses.
- Run `pytest -q` locally before pushing; attach coverage summaries in PRs when meaningful.

## Commit & Pull Request Guidelines
- Commits follow an imperative voice similar to `Initial add`; limit the subject to ~60 characters and explain motivation in the body if needed.
- Squash exploratory commits before review. Reference related issues in the body (`Refs #123`) and include sample CLI output for behavior changes.
- PR descriptions should list functional changes, test evidence, and any configuration steps reviewers must perform.

## Security & Configuration Tips
- Keep API keys outside the repo: export `POLYGON_API_KEY` or use a `.env` file ignored by Git.
- When testing, cap requests with `--limit` and `--max-pages` to avoid rate limits and reduce log noise.
- Remove or redact downloaded CSVs before sharing logs or archives.
