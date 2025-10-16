-- Partition-predicate filter + group-by
SELECT l_returnflag, l_linestatus, COUNT(*) AS cnt
FROM {{tbl}}
WHERE l_returnflag = 'R' AND l_linestatus = 'O'
GROUP BY l_returnflag, l_linestatus;
