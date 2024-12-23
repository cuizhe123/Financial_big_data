from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import unix_timestamp, col,to_date,date_format

# 创建SparkSession
spark = SparkSession.builder.appName("PurchaseRedeemPrediction").getOrCreate()

# 加载数据
data = spark.read.csv("file:///home/czz/projects/python_project/result3.1/total_balance.csv", header=True, inferSchema=True)

# 将整数类型的 report_date 转换为字符串类型，并格式化为日期格式
data = data.withColumn("report_date_str", to_date(col("report_date").cast("string"), "yyyyMMdd"))

# 转换日期格式为数值：日期转换为自某一基准日期以来的天数
data = data.withColumn("date_num", unix_timestamp("report_date_str", "yyyy-MM-dd").cast("long") )

# 显示数据结构
data.show(5)

# 构建特征向量
assembler = VectorAssembler(inputCols=["date_num"], outputCol="features")
assembled_data = assembler.transform(data)

# 显示处理后的数据
assembled_data.select("report_date", "features", "total_purchase", "total_redeem").show(5)


# 训练回归模型：预测申购总额
lr_purchase = LinearRegression(featuresCol="features", labelCol="total_purchase")
lr_model_purchase = lr_purchase.fit(assembled_data)

# 训练回归模型：预测赎回总额
lr_redeem = LinearRegression(featuresCol="features", labelCol="total_redeem")
lr_model_redeem = lr_redeem.fit(assembled_data)

import pandas as pd

# 生成2014年9月1日至9月30日的日期
dates_september = pd.date_range('2014-09-01', '2014-09-30').strftime('%Y-%m-%d').tolist()

# 创建DataFrame并转换日期为数字格式
df_september = spark.createDataFrame([(date,) for date in dates_september], ["date"])
df_september = df_september.withColumn("date_num", unix_timestamp("date", "yyyy-MM-dd").cast("long"))

# 生成特征向量
df_september_assembled = assembler.transform(df_september)

# 使用模型进行预测
forecast_purchase = lr_model_purchase.transform(df_september_assembled)
forecast_redeem = lr_model_redeem.transform(df_september_assembled)

# 显示预测结果
# forecast_purchase.select("date", "prediction").show()
# forecast_redeem.select("date", "prediction").show()



# 合并预测结果，生成期望的输出格式
forecast_purchase_redeem = forecast_purchase.join(forecast_redeem, on="date", how="inner") \
    .select(date_format("date", "yyyyMMdd").alias("date"),
            forecast_purchase["prediction"].cast("int").alias("total_purchase"), 
            forecast_redeem["prediction"].cast("int").alias("total_redeem"))

# 保存为CSV文件，不带header
forecast_purchase_redeem.select("date", "total_purchase", "total_redeem") \
    .write.option("header", "false").csv("file:///home/czz/projects/python_project/result3.2")
