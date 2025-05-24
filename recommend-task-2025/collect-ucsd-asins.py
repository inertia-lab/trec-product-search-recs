"""
Collect known ASINs from the UCSD Amazon data.

Usage:
    collect-ucsd-asins.py [-v]

Options:
    -v, --verbose
        Turn on verbose logging.
"""

import logging
import sys
from docopt import docopt
from duckdb import connect, DuckDBPyConnection

log = logging.getLogger('trec-product.collect')

def main(options):
    level = logging.DEBUG if options['--verbose'] else logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr)
    with connect() as db:
        db.execute('SET enable_progress_bar = TRUE')
        collect_asins(db)


def collect_asins(db: DuckDBPyConnection):
    log.info("copying ASINs")
    db.execute(
        """
        COPY (
            SELECT DISTINCT parent_asin AS asin
            FROM 'ucsd-2023/meta_*.jsonl.zst'
            ORDER BY asin
        ) TO 'ucsd-asins.parquet' (COMPRESSION ZSTD)
        """
    )


if __name__ == '__main__':
    opts = docopt(__doc__ or "")
    main(opts)
