crc: Cyclic Redundancy Check


# 1) inspect
python3 scripts/hudi_tool.py inspect data/amazon/hudi_user_time/hudi_linear/.hoodie --format text

# 2) footer min/max
python3 scripts/hudi_tool.py footer-minmax --table-root data/amazon/hudi_user_time/hudi_linear --column user_id
python3 scripts/hudi_tool.py footer-minmax --table-root data/amazon/hudi_user_time/hudi_zorder --column user_id
python3 scripts/hudi_tool.py footer-minmax --table-root data/amazon/hudi_user_time/hudi_no_layout --column user_id


python3 scripts/hudi_tool.py rowgroup-check \
  --table-root data/amazon/hudi_user_time/hudi_linear \
  --partition Automotive \
  --column user_id \
  --predicate prefix \
  --value AG \
  --show-reason


python3 scripts/hudi_tool.py rowgroup-check \
  --table-root data/amazon/hudi_user_time/hudi_linear \
  --partition CDs_and_Vinyl \
  --column user_id \
  --predicate none



# 3) metadata min/max (Spark + Hudi)
./scripts/start_pyspark_hudi.sh /home/unimelb.edu.au/guanlil1/Documents/lakehouse/scripts/hudi_tool.py \
  metadata-minmax --table-root /home/unimelb.edu.au/guanlil1/Documents/lakehouse/data/amazon/hudi_user_time/hudi_linear --column rating

# 4) SparkSQL query
./scripts/start_pyspark_hudi.sh \
  --conf spark.driver.memory=32g \
  --conf spark.executor.memory=32g \
  --conf spark.executor.memoryOverhead=16g \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.default.parallelism=32 \
  /home/unimelb.edu.au/guanlil1/Documents/lakehouse/scripts/hudi_tool.py \
  sparksql \
  --table-root /home/unimelb.edu.au/guanlil1/Documents/lakehouse/data/amazon/hudi_user_time/hudi_linear \
  --view-name hudi_tbl \
  --sort-cols record_timestamp,rating \
  --query "SELECT user_id FROM hudi_tbl WHERE user_id LIKE 'AG%'" \
  --query "SELECT user_id, count(*) as cnt FROM hudi_tbl WHERE user_id LIKE 'AG%'" group by user_id \
  --skip-count \
  --stats-on-preview \
  --preview-rows 20



./scripts/start_pyspark_hudi.sh \
  /home/unimelb.edu.au/guanlil1/Documents/lakehouse/scripts/hudi_tool.py \
  sparksql \
  --table-root /home/unimelb.edu.au/guanlil1/Documents/lakehouse/data/amazon/hudi_user_time/hudi_linear \
  --view-name hudi_tbl \
  --query "SELECT user_id, asin, rating, record_timestamp FROM hudi_tbl WHERE category = 'CDs_and_Vinyl' AND user_id LIKE 'AG%' LIMIT 100" \
  --skip-table-count \
  --skip-sort-minmax \
  --skip-count \
  --skip-column-stats






# 5) SparkSQL query in terminal
cd /home/unimelb.edu.au/guanlil1/Documents/lakehouse
source ~/.lakehouse/env

spark-sql \
  --packages "$HUDI_PKG" \
  --conf "spark.jars.ivy=$(pwd)/.ivy2" \
  --driver-memory 16g \
  --conf spark.executor.memory=16g \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=96 \
  --conf spark.sql.files.maxPartitionBytes=64m \
  --conf spark.sql.parquet.enableVectorizedReader=false

CREATE OR REPLACE TEMP VIEW t_no_layout
USING hudi
OPTIONS (path '/home/unimelb.edu.au/guanlil1/Documents/lakehouse/data/amazon/hudi_user_time/hudi_no_layout');

CREATE OR REPLACE TEMP VIEW t_zorder
USING hudi
OPTIONS (path '/home/unimelb.edu.au/guanlil1/Documents/lakehouse/data/amazon/hudi_user_time/hudi_zorder');

CREATE OR REPLACE TEMP VIEW t_linear
USING hudi
OPTIONS (path '/home/unimelb.edu.au/guanlil1/Documents/lakehouse/data/amazon/hudi_user_time/hudi_linear');


EXPLAIN COST
SELECT user_id, asin, rating, record_timestamp FROM t_zorder WHERE category = 'CDs_and_Vinyl' AND user_id LIKE 'AE%' LIMIT 100;
EXPLAIN COST
EXPLAIN FORMATTED
SELECT user_id, asin, rating, record_timestamp FROM t_linear WHERE category = 'CDs_and_Vinyl' AND user_id LIKE 'AE%' LIMIT 100;

EXPLAIN COST
SELECT user_id, asin, rating, record_timestamp FROM t_no_layout WHERE category = 'CDs_and_Vinyl' AND user_id LIKE 'AF%' LIMIT 100;

SELECT user_id, count(user_id) FROM t_linear WHERE category = 'CDs_and_Vinyl' AND LTRIM(user_id) LIKE 'AE222%' group by user_id LIMIT 10;

SELECT user_id, count(user_id) FROM t_zorder WHERE category = 'CDs_and_Vinyl' AND LTRIM(user_id) LIKE 'AE222%' group by user_id LIMIT 10;

SELECT user_id, count(user_id) FROM t_no_layout WHERE category = 'CDs_and_Vinyl' AND LTRIM(user_id) LIKE 'AE222%' group by user_id LIMIT 10;

