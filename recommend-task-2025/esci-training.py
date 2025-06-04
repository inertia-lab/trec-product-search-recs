"""
Generate training and validation data from ESCI data.

Usage:
    esci-training.py [-v] [-C CORPUS] [-Q FILE] [-R FILE]

Options:
    -v, --verbose
        Turn on verbose logging.
    -C CORPUS, --corpus=CORPUS
        Path to the corpus file to limit query data.
    -Q FILE, --query-output=FILE
        Write queries to FILE [default: train-esci.queries.gz]
    -R FILE, --qrel-output=FILE
        Write qrels to FILE [default: train-esci.qrels.gz]
"""

import logging
import sys

from docopt import docopt
from duckdb import DuckDBPyConnection, connect
from xopen import xopen

log = logging.getLogger("trec-product.esci-training")

ESCI_EXAMPLES = (
    "esci-data/shopping_queries_dataset/shopping_queries_dataset_examples.parquet"
)


def main(options):
    level = logging.DEBUG if options["--verbose"] else logging.INFO
    logging.basicConfig(level=level, stream=sys.stderr)
    with connect() as db:
        load_reference(db, options.get("--corpus"))
        load_examples(db)
        summarize_examples(db)
        find_item_relationships(db)
        write_items(db, options["--query-output"])
        write_qrels(db, options["--qrel-output"])


def load_reference(db: DuckDBPyConnection, corpus_path: str | None):
    if corpus_path is not None:
        log.info("reading corpus from %s", corpus_path)
        db.execute(
            f"CREATE TABLE valid_asins AS SELECT id AS asin FROM '{corpus_path}'"
        )
    else:
        log.info("loading UCSD ASINSs")
        db.execute("CREATE TABLE valid_asins AS SELECT asin FROM 'ucsd-asins.parquet'")
    db.execute("CREATE INDEX valid_asin_idx ON valid_asins (asin)")


def load_examples(db: DuckDBPyConnection):
    log.info("loading %s", ESCI_EXAMPLES)
    db.execute(f"CREATE TABLE esci_queries AS select * from '{ESCI_EXAMPLES}'")
    db.execute("SELECT COUNT(*) FROM esci_queries")
    (count,) = db.fetchone()
    log.info("loaded %d example entries", count)


def summarize_examples(db: DuckDBPyConnection):
    db.execute(
        """
        CREATE TABLE query_stats AS
        WITH counts AS (
            SELECT query_id, query, esci_label, COUNT(*) AS N
            FROM esci_queries
            SEMI JOIN valid_asins ON (product_id = asin)
            WHERE product_locale = 'us'
            GROUP BY query_id, query, esci_label
        )
        PIVOT counts
        ON esci_label
        USING SUM(N)
        """
    )


def find_item_relationships(db: DuckDBPyConnection):
    log.info("collecting item relationships")
    db.execute(
        """
        CREATE TYPE product_relation AS ENUM('I', 'S', 'C');
        CREATE TABLE item_relationships (
            ref_asin VARCHAR NOT NULL,
            tgt_asin VARCHAR NOT NULL,
            rel_type product_relation
        );
        """
    )
    db.execute(
        """
        WITH usable_queries AS (
            SELECT query_id, product_id, esci_label
            FROM esci_queries
            SEMI JOIN valid_asins ON (product_id = asin)
            WHERE product_locale = 'us'
        )
        INSERT INTO item_relationships (ref_asin, tgt_asin, rel_type)
        SELECT rq.product_id AS ref_asin, tq.product_id AS tgt_asin, IF(tq.esci_label = 'E', 'S', tq.esci_label) AS rel_type
        FROM usable_queries rq, usable_queries tq
        WHERE rq.query_id = tq.query_id
        AND rq.esci_label = 'E'
        AND tq.product_id <> rq.product_id
        """
    )
    print(db.table("item_relationships").to_df())
    log.info("counting relationships")
    db.execute(
        """
        CREATE TABLE item_summary AS
        PIVOT item_relationships
        ON rel_type
        USING COUNT(*)
        GROUP BY ref_asin
        """
    )
    db.execute(
        """
        CREATE VIEW train_items AS
        SELECT row_number() OVER (ORDER BY ref_asin) AS q_id, ref_asin, C, S, I
        FROM item_summary WHERE C >=5 AND S >= 5
        """
    )
    print(db.table("train_items").to_df())


def write_items(db: DuckDBPyConnection, out_fn: str):
    log.info("Writing queries to %s", out_fn)
    with xopen(out_fn, "wt") as outf:
        db.execute(
            """
            SELECT DISTINCT q_id || rel_type AS qno, ref_asin
            FROM train_items
            JOIN item_relationships USING (ref_asin)
            WHERE rel_type IN ('C', 'S')
            ORDER BY q_id, rel_type
            """
        )

        n = 0
        for row in db.fetchall():
            print("\t".join(str(c) for c in row), file=outf)
            n += 1
        log.info("Wrote %d queries", n)


def write_qrels(db: DuckDBPyConnection, out_fn: str):
    log.info("Writing qrels to %s", out_fn)
    with xopen(out_fn, "wt") as outf:
        db.execute(
            """
            SELECT q_id || 'C' AS qno, 0 AS iter, tgt_asin, IF(rel_type = 'C', 1, 0) AS rel
            FROM train_items
            JOIN item_relationships USING (ref_asin)
            ORDER BY q_id, rel_type, tgt_asin
            """
        )

        for row in db.fetchall():
            print("\t".join(str(c) for c in row), file=outf)

        db.execute(
            """
            SELECT q_id || 'S' AS qno, 0 AS iter, tgt_asin, IF(rel_type = 'S', 1, 0) AS rel
            FROM train_items
            JOIN item_relationships USING (ref_asin)
            ORDER BY q_id, rel_type, tgt_asin
            """
        )

        for row in db.fetchall():
            print("\t".join(str(c) for c in row), file=outf)


if __name__ == "__main__":
    opts = docopt(__doc__ or "")
    main(opts)
