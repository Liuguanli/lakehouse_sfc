-- ORDER BY + LIMIT (forces wide shuffle if no ordering)
SELECT l_orderkey, l_shipdate, l_extendedprice
FROM {{tbl}}
ORDER BY l_shipdate DESC, l_orderkey
LIMIT 1000;
