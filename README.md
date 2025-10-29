# Polygon Futures Trade Downloader

CLI tool that downloads futures trade data from the Polygon.io REST API and
saves each run to a CSV file for further analysis.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Export a valid Polygon API key before running commands:

```bash
export POLYGON_API_KEY="your-secret-key"
```

## Usage

```bash
python3 polygon_downloader.py ESZ5 2025-08-22 \
  --api-key "$POLYGON_API_KEY" \
  --max-pages 1
```

Key flags:
- `--limit`: results per page (default 50000).
- `--max-pages`: cap pagination to avoid quota surprises.
- `--sort`: `timestamp.asc` or `timestamp.desc` (default).
- `--output`: override default `<ticker>_<date>.csv`.

Run `python3 polygon_downloader.py --help` to review all options and examples.

## Data Management

By default, CSV files are saved in the current directory as `<ticker>_<date>.csv`.
Use the `--output` flag to specify a custom path (e.g., `data/ESZ5_2025-08-22.csv`).

Keep API keys and generated data out of version control. Remove or redact
downloads before sharing logs or archives.

## Testing

Tests are not yet implemented. Future work will add a `tests/` directory with
`pytest` and HTTP mocking tools to cover pagination, throttling, and error paths.
