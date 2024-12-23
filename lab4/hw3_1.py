from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("Daily Purchase and Redeem Analysis") \
    .getOrCreate()

# 读取数据，假设文件在 CSV 格式
data_path = "file:///home/czz/info/Ali/user_balance_table.csv"
user_balance_df = spark.read.csv(data_path, header=True, inferSchema=True)

# 按天汇总申购和赎回总额
result_df = user_balance_df.groupBy("report_date").agg(
    spark_sum("total_purchase_amt").alias("total_purchase"),
    spark_sum("total_redeem_amt").alias("total_redeem")
)

# 排序输出
result_df = result_df.orderBy("report_date")

# 展示结果
result_df.show()

# 如果需要保存结果到文件
result_df.write.csv("file:///home/czz/projects/python_project/result3.1", header=True)

# 停止 SparkSession
spark.stop()