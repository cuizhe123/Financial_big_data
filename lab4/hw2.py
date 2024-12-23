from pyspark.sql import SparkSession

# 初始化 SparkSession
spark = SparkSession.builder.appName("User Analysis").getOrCreate()

# 加载 user_balance_table
user_balance_table = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///home/czz/projects/python_project/user_balance_table.csv")
user_balance_table.createOrReplaceTempView("user_balance_table")

# 加载 user_profile_table
user_profile_table = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///home/czz/projects/python_project//user_profile_table.csv")
user_profile_table.createOrReplaceTempView("user_profile_table")

query1 = """
SELECT 
    p.City AS city_id,
    ROUND(AVG(b.tBalance), 2) AS avg_balance
FROM 
    user_balance_table b
JOIN 
    user_profile_table p
ON 
    b.user_id = p.user_id
WHERE 
    b.report_date = '20140301'
GROUP BY 
    p.City
ORDER BY 
    avg_balance DESC
"""
result1 = spark.sql(query1)
result1.show()

query2 = """
WITH city_user_flow AS (
    SELECT 
        p.City AS city_id,
        b.user_id,
        SUM(b.total_purchase_amt + b.total_redeem_amt) AS total_flow
    FROM 
        user_balance_table b
    JOIN 
        user_profile_table p
    ON 
        b.user_id = p.user_id
    WHERE 
        b.report_date LIKE '201408%' -- 2014 年 8 月的数据
    GROUP BY 
        p.City, b.user_id
),
city_top3_flow AS (
    SELECT 
        city_id, 
        user_id, 
        total_flow,
        ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY total_flow DESC) AS rank
    FROM 
        city_user_flow
)
SELECT 
    city_id,
    user_id,
    total_flow
FROM 
    city_top3_flow
WHERE 
    rank <= 3
"""
result2 = spark.sql(query2)
result2.show()



result1.write.csv("file:///home/czz/projects/python_project/result2.1", header=True)
result2.write.csv("file:///home/czz/projects/python_project/result2.2", header=True)
