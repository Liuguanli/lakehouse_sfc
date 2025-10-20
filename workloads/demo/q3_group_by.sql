-- Aggregation with group by
SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, avg(l_extendedprice) AS avg_price
FROM {{tbl}}
GROUP BY l_returnflag, l_linestatus;
