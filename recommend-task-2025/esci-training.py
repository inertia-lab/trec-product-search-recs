"""
Generate training and validation data from ESCI data.

Usage:
    esci-training.py [-v]

Options:
    -v, --verbose
        Turn on verbose logging.
"""

import logging
import sys
from docopt import docopt
from duckdb import connect, DuckDBPyConnection

log = logging.getLogger('trec-product.esci-training')

ESCI_EXAMPLES = "esci-data/shopping_queries_dataset/shopping_queries_dataset_examples.parquet"

def main(options):
    level = logging.DEBUG if options['--verbose'] else logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr)
    with connect() as db:
        load_reference(db)
        load_examples(db)
        summarize_examples(db)

def load_reference(db: DuckDBPyConnection):
    log.info("loading UCSD ASINSs")
    db.execute("CREATE TABLE ucsd_asins AS SELECT asin FROM 'ucsd-asins.parquet'")
    db.execute("CREATE INDEX uscd_asin_idx ON ucsd_asins (asin)")

def load_examples(db: DuckDBPyConnection):
    log.info("loading %s", ESCI_EXAMPLES)
    db.execute(f"CREATE TABLE esci_queries AS select * from '{ESCI_EXAMPLES}'")
    db.execute("SELECT COUNT(*) FROM esci_queries")
    count, = db.fetchone()
    log.info("loaded %d example entries", count)

def summarize_examples(db: DuckDBPyConnection):
    db.execute(
        """
        CREATE TABLE query_stats AS
        WITH counts AS (
                SELECT query_id, query, esci_label, COUNT(*) AS N
                FROM esci_queries
                SEMI JOIN ucsd_asins ON (product_id = asin)
                WHERE product_locale = 'us'
                GROUP BY query_id, query, esci_label
        )
        PIVOT counts
        ON esci_label
        USING SUM(N)
        """)
    print(db.table('query_stats').df())


if __name__ == '__main__':
    opts = docopt(__doc__ or "")
    main(opts)
