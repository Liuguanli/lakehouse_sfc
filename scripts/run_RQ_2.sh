

# Column Selection and ordering

# 1、 Number of SFC columns (𝑘 = 2..5) vs. file pruning/ row-group pruning

# 3、 Attribute correlation vs. SFC improvement (heatmap or scatter).

# 4、Optional Table: Summary of column-subset performance across datasets

# Test TPCH-16  2 columns (maybe different combinations, correlated or not)
# Test Amazon-review  2 columns (maybe different combinations, correlated or not)
# Test TPCH-16  3,4,5 columns (maybe different combinations, correlated or not)



# 尝试相同的列不同的顺序 看是否影响

# 对于2列，和3列的情况，画出不同的顺序。看看顺序对 pruning effectiveness的影响  这里我们
# 这里我先试用TPC-H 16
# 画出 pruning effectiveness vs. column correlation的图



# 尝试不同 selectivity， 尝试2,3,4,5 列 组合 尝试的查询也需要修改。 可以是查询跟列有关，也可以是无关
# 1 写 TPCH-16，2，生成查询，3，执行查询， 
# 重复 TPCH-16， 用 其他2列， 一共选择 n组，然后是3列，4列，5列
# 1 写 Amazon-review，2，生成查询，3，执行查询，用 其他2列， 一共选择 n组

