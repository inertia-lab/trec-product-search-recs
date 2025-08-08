"""
Collect known ASINs from the UCSD Amazon data.

Note: this expects the data to be ZSTD-compressed.

Usage:
    collect-ucsd-asins.py [-v] DIR

Options:
    -v, --verbose
        Turn on verbose logging.
    DIR
        Path to the directory containing Amazon meatadata.
"""

import logging
import sys

from docopt import docopt
from duckdb import DuckDBPyConnection, connect

log = logging.getLogger("trec-product.collect")


def main(options):
    level = logging.DEBUG if options["--verbose"] else logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr)
    with connect() as db:
        db.execute("SET enable_progress_bar = TRUE")
        collect_asins(db, options["DIR"])


def collect_asins(db: DuckDBPyConnection, path: str):
    log.info("copying ASINs")
    db.execute(
        f"""
        COPY (
            SELECT DISTINCT
                regexp_extract(filename, 'meta_(.*)\\.jsonl\\.zst', 1) AS category,
                parent_asin AS asin,
                len(description) AS n_desc_entries,
                len(description[1]) AS desc_len,
                rating_number AS rating_count,
            FROM read_json('{path}/meta_*.jsonl.zst', filename=true)
            ORDER BY asin
        ) TO 'ucsd-asins.parquet' (COMPRESSION ZSTD)
        """
    )


if __name__ == "__main__":
    opts = docopt(__doc__ or "")
    main(opts)
