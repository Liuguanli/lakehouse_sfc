# Iceberg

source ~/.lakehouse/env

TS=$(date +%Y%m%d_%H%M%S)

$SPARK_HOME/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse="$(pwd)/data/iceberg_wh" \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine iceberg \
  --table local.demo.events_iceberg_baseline \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_iceberg_baseline_${TS}.csv"

$SPARK_HOME/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse="$(pwd)/data/iceberg_wh" \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine iceberg \
  --table local.demo.events_iceberg_linear \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_iceberg_linear_${TS}.csv"

$SPARK_HOME/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse="$(pwd)/data/iceberg_wh" \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine iceberg \
  --table local.demo.events_iceberg_zorder \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_iceberg_zorder_${TS}.csv"


# Delta

TS=$(date +%Y%m%d_%H%M%S)
$SPARK_HOME/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine delta \
  --table "$(pwd)/data/delta/delta_baseline" \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_delta_baseline_${TS}.csv"

# Delta - linear
$SPARK_HOME/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine delta \
  --table "$(pwd)/data/delta/delta_linear" \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_delta_linear_${TS}.csv"

# Delta - zorder
$SPARK_HOME/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine delta \
  --table "$(pwd)/data/delta/delta_zorder" \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_delta_zorder_${TS}.csv"


# Hudi
TS=$(date +%Y%m%d_%H%M%S)

# HUDI — no_layout
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine hudi \
  --table "$(pwd)/data/hudi/hudi_no_layout" \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_hudi_no_layout_${TS}.csv"

# HUDI — linear
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine hudi \
  --table "$(pwd)/data/hudi/hudi_linear" \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_hudi_linear_${TS}.csv"

# HUDI — zorder
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine hudi \
  --table "$(pwd)/data/hudi/hudi_zorder" \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_hudi_zorder_${TS}.csv"

# HUDI — hilbert
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.files.maxPartitionBytes=256m \
  --conf spark.driver.memory=16g \
  --conf spark.executor.memory=16g \
  --conf spark.executor.memoryOverhead=4g \
  lakehouse_op/run_queries.py \
  --engine hudi \
  --table "$(pwd)/data/hudi/hudi_hilbert" \
  --queries_dir workloads/demo \
  --warmup \
  --cache none \
  --action count \
  --output_csv "results_hudi_hilbert_${TS}.csv"
