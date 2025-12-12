

# Scalability with Data Volume and File Counts

# TPCH single, varying data scale (1,4,16,64)

# 1. use typical queries (low, medium, high selectivity) and different templates!!!

# 2. Try different configs if possible.


# 这个里面要根据前面的结果来做到底选择什么列 什么查询

# 写 TPCH-1, 生成查询（多个selectivity），执行查询，删除数据
# 写 TPCH-4, 生成查询（多个selectivity），执行查询，删除数据
# 写 TPCH-64, 生成查询（多个selectivity），执行查询，删除数据 (每个layout 分别执行)

