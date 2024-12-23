
from pyspark import SparkContext

# 初始化SparkContext
sc = SparkContext("local", "FinancialFlowAnalysis")

# 加载数据
lines = sc.textFile("file:///home/czz/projects/python_project/user_balance_table.csv")
header = lines.first()  # 获取第一行
data = lines.filter(lambda line: line != header)  # 过滤标题行

# 解析CSV数据，假设数据字段用逗号分隔
def parse_line(line):
    fields = line.split(",")
    report_date = fields[1]  # 假设 report_date 在第二列
    total_purchase_amt = float(fields[4])  # 总购买金额
    total_redeem_amt = float(fields[9])  # 总赎回金额
    return (report_date, total_purchase_amt, total_redeem_amt)

# 解析数据并映射为 (日期, 资金流入, 资金流出) 元组
parsed_data = data.map(parse_line)

# 按照日期进行聚合，计算每一天的总资金流入和流出
daily_flow = parsed_data.map(lambda x: (x[0], (x[1], x[2]))) \
                        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# 按日期排序
sorted_daily_flow = daily_flow.sortByKey()

# 格式化输出结果
result = sorted_daily_flow.map(lambda x: f"{x[0]} {x[1][0]:.2f} {x[1][1]:.2f}")

# 输出到文件
output_path = "file:///home/czz/projects/python_project/output1_1"
result.saveAsTextFile(output_path)
# result.collect()  # 触发计算

print(f"结果已保存到 {output_path}")


import os

output_dir = "/home/czz/projects/python_project/output1_1"
final_output_file = "/home/czz/projects/python_project/final_result1_1.txt"

# 合并所有 part 文件
with open(final_output_file, "w") as outfile:
    for filename in sorted(os.listdir(output_dir)):
        if filename.startswith("part-"):
            with open(os.path.join(output_dir, filename), "r") as infile:
                outfile.write(infile.read())

print(f"结果已整合到 {final_output_file}")


