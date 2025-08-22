CREATE TABLE corpus AS
SELECT * FROM 'product-corpus.json.zst';

COPY (
    SELECT row_number() OVER (ORDER BY asin) AS qid, asin, product_type, title
    FROM 'query_products.csv' qp
    JOIN corpus c ON qp.asin = c.id
    ORDER BY asin
) TO 'rec-queries.tsv' (HEADER FALSE, DELIM '\t');
