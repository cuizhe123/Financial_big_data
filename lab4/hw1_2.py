from pyspark import SparkContext

# 初始化 SparkContext
sc = SparkContext("local", "User Balance Analysis")

# 加载数据（文件路径替换为实际路径）
data_path = "file:///home/czz/projects/python_project/user_balance_table.csv"
rdd = sc.textFile(data_path)

# 解析数据，忽略表头
header = rdd.first()
data_rdd = rdd.filter(lambda line: line != header).map(lambda line: line.split(','))

# 提取需要的字段：report_date, total_purchase_amt, total_redeem_amt
# 假设字段顺序对应：report_date 在第 1 列，total_purchase_amt 在第 5 列，total_redeem_amt 在第 9 列
# 转换为 (date, (流入量, 流出量)) 格式


## 任务 2: 活跃用户分析
# 提取 user_id 和日期，构建 (user_id, date) 对
user_date_rdd = data_rdd.map(lambda x: (x[0], x[1]))

# 过滤指定月份的数据（2014 年 8 月）
active_user_rdd = user_date_rdd.filter(lambda x: x[1].startswith("201408"))

# 按 user_id 分组，并统计每个用户的活跃天数
user_active_days_rdd = active_user_rdd.distinct().groupByKey().mapValues(len)

# 筛选出活跃天数 >= 5 的用户
active_users_count = user_active_days_rdd.filter(lambda x: x[1] >= 5).count()

print("\n2014 年 8 月的活跃用户总数：")
print(active_users_count)

# 停止 SparkContext
sc.stop()