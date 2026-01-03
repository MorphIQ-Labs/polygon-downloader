#!/usr/bin/env bash
#
# Download Polygon.io futures trades for a date range.
# Each day is saved to a separate CSV file.
#
# Usage:
#   ./download_range.sh TICKER START_DATE END_DATE [OPTIONS]
#
# Example:
#   ./download_range.sh ESZ5 2025-08-20 2025-08-25 --max-pages 1
#   ./download_range.sh ESZ5 2025-08-20 2025-08-25 --limit 10000 --output-dir data/

set -euo pipefail

# Check arguments
if [ $# -lt 3 ]; then
    echo "Usage: $0 TICKER START_DATE END_DATE [OPTIONS]" >&2
    echo "" >&2
    echo "Arguments:" >&2
    echo "  TICKER      Futures ticker symbol (e.g., ESZ5)" >&2
    echo "  START_DATE  Start date in YYYY-MM-DD format" >&2
    echo "  END_DATE    End date in YYYY-MM-DD format (inclusive)" >&2
    echo "  OPTIONS     Additional flags passed to polygon_downloader.py" >&2
    echo "" >&2
    echo "Environment:" >&2
    echo "  POLYGON_API_KEY  Required. Set your Polygon.io API key." >&2
    echo "" >&2
    echo "Examples:" >&2
    echo "  $0 ESZ5 2025-08-20 2025-08-25" >&2
    echo "  $0 ESZ5 2025-08-20 2025-08-25 --max-pages 1" >&2
    echo "  $0 ESZ5 2025-08-20 2025-08-25 --limit 10000 --output-dir data/" >&2
    exit 1
fi

# Check for API key
if [ -z "${POLYGON_API_KEY:-}" ]; then
    echo "Error: POLYGON_API_KEY environment variable is not set" >&2
    exit 1
fi

TICKER="$1"
START_DATE="$2"
END_DATE="$3"
shift 3

# Extract output directory if specified, default to current directory
OUTPUT_DIR="."
EXTRA_ARGS=()
while [ $# -gt 0 ]; do
    if [[ "$1" == "--output-dir" ]]; then
        OUTPUT_DIR="$2"
        shift 2
    else
        EXTRA_ARGS+=("$1")
        shift
    fi
done

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Validate date formats
if ! date -j -f "%Y-%m-%d" "$START_DATE" "+%Y-%m-%d" >/dev/null 2>&1; then
    echo "Error: Invalid START_DATE format '$START_DATE'. Use YYYY-MM-DD" >&2
    exit 1
fi

if ! date -j -f "%Y-%m-%d" "$END_DATE" "+%Y-%m-%d" >/dev/null 2>&1; then
    echo "Error: Invalid END_DATE format '$END_DATE'. Use YYYY-MM-DD" >&2
    exit 1
fi

# Convert dates to Unix timestamps for iteration (macOS compatible)
START_TS=$(date -j -f "%Y-%m-%d" "$START_DATE" "+%s")
END_TS=$(date -j -f "%Y-%m-%d" "$END_DATE" "+%s")

if [ "$START_TS" -gt "$END_TS" ]; then
    echo "Error: START_DATE must be before or equal to END_DATE" >&2
    exit 1
fi

# Calculate number of days
DAYS=$(( (END_TS - START_TS) / 86400 + 1 ))

echo "Downloading $TICKER trades from $START_DATE to $END_DATE ($DAYS days)"
echo "Output directory: $OUTPUT_DIR"
if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
    echo "Extra arguments: ${EXTRA_ARGS[*]}"
else
    echo "Extra arguments: none"
fi
echo ""

# Iterate through each day
CURRENT_TS=$START_TS
DAY_NUM=1

while [ "$CURRENT_TS" -le "$END_TS" ]; do
    # Format current date
    CURRENT_DATE=$(date -j -f "%s" "$CURRENT_TS" "+%Y-%m-%d")
    OUTPUT_FILE="$OUTPUT_DIR/${TICKER}_${CURRENT_DATE}.csv"

    echo "[$DAY_NUM/$DAYS] Downloading $CURRENT_DATE..."

    # Run the downloader
    if [ "${#EXTRA_ARGS[@]}" -gt 0 ]; then
        python3 polygon_downloader.py "$TICKER" "$CURRENT_DATE" \
            --api-key "$POLYGON_API_KEY" \
            --output "$OUTPUT_FILE" \
            "${EXTRA_ARGS[@]}"
    else
        python3 polygon_downloader.py "$TICKER" "$CURRENT_DATE" \
            --api-key "$POLYGON_API_KEY" \
            --output "$OUTPUT_FILE"
    fi

    if [ $? -eq 0 ]; then
        echo "[$DAY_NUM/$DAYS] ✓ Completed $CURRENT_DATE"
    else
        echo "[$DAY_NUM/$DAYS] ✗ Failed $CURRENT_DATE" >&2
    fi

    echo ""

    # Move to next day
    CURRENT_TS=$((CURRENT_TS + 86400))
    DAY_NUM=$((DAY_NUM + 1))
done

echo "Download complete. Files saved to: $OUTPUT_DIR"
