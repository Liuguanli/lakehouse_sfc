Scripts to run SQL workloads against **Delta**, **Hudi**, and **Iceberg** on Spark.

source ~/.lakehouse/env

## Environmental Setup:
```bash
bash scripts/lakehouse_setup.sh --repair --use-sudo
```

## Layout
- `lakehouse_op/` — Python runner(s) like `run_queries.py`
- `workloads/` — SQL files using `{{tbl}}` placeholder
- `scripts/` — setup helpers
- `data/` *(ignored)* — datasets
- `metastore_db/` *(ignored)* — local Spark metastore
- `results_*.csv` — run outputs

## Example (Delta baseline)
```bash
TS=$(date +%Y%m%d_%H%M%S)
$SPARK_HOME/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  lakehouse_op/run_queries.py \
  --engine delta \
  --table "\$(pwd)/data/delta/delta_baseline" \
  --queries_dir workloads \
  --warmup --cache none --action count \
  --output_csv "results_delta_baseline_\${TS}.csv"
```

  ## References

  https://docs.delta.io/optimizations-oss/#optimize-performance-with-file-management


## commands

nohup bash ./scripts/run_benchmark_tpch_all.sh > tpch_all.log 2>&1 &
nohup bash ./scripts/run_benchmark_tpch_all.sh > /dev/null 2> tpch_err.log &


nohup bash ./scripts/run_RQ_1_matrix.sh > tpch_all.log 2>&1 &
nohup bash ./scripts/run_RQ_1_matrix.sh > /dev/null 2> tpch_err.log &

bash ./scripts/run_RQ_6_matrix.sh
nohup bash ./scripts/run_RQ_6_matrix.sh > tpch_all.log 2>&1 &


[1] 1374045
ps aux | grep run_RQ_6_matrix.sh



kill -9 2196891