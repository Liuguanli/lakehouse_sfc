crc: Cyclic Redundancy Check


# 1) inspect
python3 scripts/hudi_tool.py inspect data/amazon/hudi_user_time/hudi_linear/.hoodie --format text

# 2) footer min/max
python3 scripts/hudi_tool.py footer-minmax --table-root data/amazon/hudi_user_time/hudi_linear --column user_id
python3 scripts/hudi_tool.py footer-minmax --table-root data/amazon/hudi_user_time/hudi_zorder --column user_id
python3 scripts/hudi_tool.py footer-minmax --table-root data/amazon/hudi_user_time/hudi_no_layout --column user_id



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
