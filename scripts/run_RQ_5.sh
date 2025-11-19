

# File pruning comparison across Delta, Hudi,
# Iceberg. todo: Add Figure: Row-group pruning and page skipping
# (where available). todo: Add Figure: Planning time and metadata
# size across systems. todo: Optional Table: Summary of cross-system
# layout differences and effects.

# Run all three engines' layout scripts with the specified parameters
# TPCH-16, TPCH-16 overall, Amazon Review  on default layouts with 2 columns

# 我们就用TPCH-16,  Amazon Review 这三个数据集
# 我们执行所有三种引擎的 layout 脚本，使用默认的2列布局
# 然后我们生成查询，执行查询，收集结果

# 我们的主要目的是分析各个系统的 某些差异，和相同点，不是来比较哪个系统更好