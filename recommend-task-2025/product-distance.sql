.print Loading qrels;
CREATE TEMPORARY TABLE qrels AS
SELECT topic, asin, grade
FROM read_csv(
    'recsys.qrels',
    delim=' ',
    columns={
        'topic': 'VARCHAR',
        'iter': 'INTEGER',
        'asin': 'VARCHAR',
        'grade': 'CHAR(2)'
    }
);

.print Expanding product categories;
CREATE TEMPORARY TABLE pcats AS
SELECT asin, UNNEST(cat_list) AS category, length(cat_list) - generate_subscripts(cat_list, 1) AS rindex,
    rindex / length(cat_list) AS rfrac
FROM products
SEMI JOIN qrels USING (asin)
WHERE cat_list IS NOT NULL;

.print Computing product distances;
COPY (
    SELECT p1.asin AS asin1, p2.asin AS asin2,
        MIN(pc1.rindex + pc2.rindex) AS distance,
        MIN(pc1.rfrac + pc2.rfrac) AS fdistance
    FROM products p1
    JOIN pcats pc1 ON p1.asin = pc1.asin
    JOIN pcats pc2 ON pc1.category = pc2.category
    JOIN products p2 ON pc2.asin = p2.asin
    GROUP BY p1.asin, p2.asin
) TO 'product-distances.csv.zst';
