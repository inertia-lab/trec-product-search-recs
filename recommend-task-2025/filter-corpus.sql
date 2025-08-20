ATTACH 'ucsd-products.duckdb' AS ucsd (READONLY);

CREATE VIEW esci_products AS
SELECT * FROM 'esci-data/shopping_queries_dataset/shopping_queries_dataset_products.parquet'
WHERE product_locale = 'us';

CREATE VIEW m2_products AS
SELECT * FROM 'amazon-m2/products_train.csv.zst'
WHERE locale = 'US';

CREATE TABLE products (
    id VARCHAR,
    title VARCHAR,
    "desc" VARCHAR,
    brand VARCHAR,
    product_bullet_point VARCHAR,
);

INSERT INTO products
SELECT product_id, product_title, product_description, product_bullet_point, product_brand
FROM esci_products;

INSERT INTO products
SELECT id, title, "desc", NULL, brand
FROM m2_products
WHERE id NOT IN (SELECT id FROM products);

DELETE FROM products
WHERE id NOT IN (
    SELECT asin
    FROM ucsd.products
    WHERE category IN ('Electronics', 'Home_and_Kitchen', 'Sports_and_Outdoors')
    AND desc_len > 50
);

COPY products TO 'product-corpus.json.zst';
