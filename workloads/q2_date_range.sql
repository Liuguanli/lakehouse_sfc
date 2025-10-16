-- Date range on layout columns
SELECT count(*) AS cnt
FROM {{tbl}}
WHERE l_shipdate >= DATE '1995-01-01'
  AND l_shipdate <  DATE '1996-01-01';
