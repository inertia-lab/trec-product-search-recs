METAS = $(wildcard ucsd-2023/meta_*.jsonl.zst)

.PHONY: data corpus queries

data: corpus queries

corpus: product-corpus.json.zst

queries: rec-queries.tsv

ucsd-products.duckdb: $(METAS)
	rm -f ucsd-products.duckdb
	duckdb -f import-meta.sql ucsd-products.duckdb

product-corpus.json.zst: ucsd-products.duckdb esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet amazon-m2/products_train.csv.zst
	duckdb -f filter-corpus.sql

rec-queries.tsv: product-corpus.json.zst # query_products.csv
	duckdb -f prep-queries.sql
