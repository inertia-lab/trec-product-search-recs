CREATE OR REPLACE SEQUENCE product_number;
CREATE OR REPLACE TABLE products (
    prod_id INTEGER PRIMARY KEY DEFAULT nextval('product_number'),
    asin VARCHAR NOT NULL UNIQUE,
    category VARCHAR NOT NULL,
    n_desc_entries INTEGER,
    desc_len INTEGER,
    rating_count INTEGER,
);

.print Loading UCSD products;
INSERT INTO products (category, asin, n_desc_entries, desc_len, rating_count)
SELECT DISTINCT
    regexp_extract(filename, 'meta_(.*)\.jsonl\.zst', 1) AS category,
    parent_asin AS asin,
    len(description) AS n_desc_entries,
    len(description[1]) AS desc_len,
    rating_number AS rating_count,
FROM read_json('ucsd-2023/meta_*.jsonl.zst', filename=true)
ORDER BY asin;

.print Loading ESCI products;
CREATE TABLE esci_products AS
SELECT product_id AS asin, product_title AS title
FROM 'esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet';

.print Loading M2 products;
CREATE TABLE m2_products AS
SELECT id AS asin, title
FROM 'amazon-m2/products_train.csv.zst';
