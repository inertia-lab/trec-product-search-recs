"""
Generate training and validation data from ESCI data.

Usage:
    esci-training.py
"""

from duckdb import connect, DuckDBPyConnection

ESCI_EXAMPLES = "esci-data/shopping_queries_dataset/shopping_queries_dataset_examples.parquet"

def main():
    with connect() as db:
        load_examples(db)

def load_examples(db: DuckDBPyConnection):
    db.execute(f"CREATE TABLE esci_queries AS select * from '{ESCI_EXAMPLES}'")


if __name__ == '__main__':
    main()
