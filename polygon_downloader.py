#!/usr/bin/env python3
"""
Polygon.io Trade Data Downloader
Downloads Futures trade data from the Polygon.io REST API
"""

import argparse
import csv
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
import requests


class PolygonFuturesDownloader:
    """Downloads Futures trade data from Polygon.io API"""

    BASE_URL = "https://api.polygon.io/futures/vX/trades"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def download_trades(
        self,
        ticker: str,
        timestamp: str,
        limit: int = 50000,
        sort: str = "timestamp.desc",
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Download trade data for a given ticker and timestamp.

        Args:
            ticker: Futures ticker symbol (e.g., 'ESZ5')
            timestamp: Date in YYYY-MM-DD format
            limit: Number of results per page (default: 50000)
            sort: Sort order (default: timestamp.desc)
            max_pages: Maximum number of pages to fetch (None = all)

        Returns:
            List of all trade records
        """
        all_trades = []
        page_count = 0

        # Build initial URL
        url = f"{self.BASE_URL}/{ticker}"
        params = {
            "apiKey": self.api_key,
            "timestamp": timestamp,
            "limit": limit,
            "sort": sort
        }

        while url:
            page_count += 1

            # Make request
            if page_count == 1:
                response = self.session.get(url, params=params)
            else:
                # next_url doesn't include API key, so we need to add it
                response = self.session.get(url, params={"apiKey": self.api_key})

            response.raise_for_status()
            data = response.json()

            # Check status
            if data.get("status") != "OK":
                print(f"API returned status: {data.get('status')}", file=sys.stderr)
                break

            # Get results
            results = data.get("results", [])
            all_trades.extend(results)

            print(f"Page {page_count}: Retrieved {len(results)} trades (total: {len(all_trades)})", file=sys.stderr)

            # Check for next page
            url = data.get("next_url")
            if url:
                print(f"Next page URL found, continuing pagination...", file=sys.stderr)
            else:
                print(f"No more pages available", file=sys.stderr)

            # Check max pages limit
            if max_pages and page_count >= max_pages:
                print(f"Reached max pages limit ({max_pages})", file=sys.stderr)
                break

        return all_trades

    def save_to_file(self, data: List[Dict[str, Any]], output_file: str):
        """Save trade data to CSV file"""
        if not data:
            print(f"No data to save", file=sys.stderr)
            return

        # Get all unique field names from the data
        fieldnames = list(data[0].keys())

        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"Saved {len(data)} trades to {output_file}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Download Futures trade data from Polygon.io",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s ESZ5 2025-08-22 --api-key YOUR_KEY
  %(prog)s ESZ5 2025-08-22 --api-key YOUR_KEY --output trades.csv
  %(prog)s ESZ5 2025-08-22 --api-key YOUR_KEY --limit 5 --max-pages 2
        """
    )

    parser.add_argument(
        "ticker",
        help="Futures ticker symbol (e.g., ESZ5)"
    )

    parser.add_argument(
        "timestamp",
        help="Date in YYYY-MM-DD format (e.g., 2025-08-22)"
    )

    parser.add_argument(
        "--api-key",
        required=True,
        help="Polygon.io API key"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=50000,
        help="Number of results per page (default: 50000)"
    )

    parser.add_argument(
        "--sort",
        default="timestamp.desc",
        choices=["timestamp.asc", "timestamp.desc"],
        help="Sort order (default: timestamp.desc)"
    )

    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV file (default: {ticker}_{timestamp}.csv)"
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum number of pages to fetch (default: all)"
    )

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.timestamp, '%Y-%m-%d')
    except ValueError:
        print(f"Error: Invalid date format '{args.timestamp}'. Use YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    # Set default output filename
    if args.output is None:
        args.output = f"{args.ticker}_{args.timestamp}.csv"

    # Download trades
    try:
        downloader = PolygonFuturesDownloader(args.api_key)
        trades = downloader.download_trades(
            ticker=args.ticker,
            timestamp=args.timestamp,
            limit=args.limit,
            sort=args.sort,
            max_pages=args.max_pages
        )

        # Save to file
        downloader.save_to_file(trades, args.output)

    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
