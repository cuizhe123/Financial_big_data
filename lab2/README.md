[TOC]

# 任务一：每日资金流入流出统计

## Mapper

1. 对传入文件进行处理，跳过第一行表头

2. 对每一行，依照tab键和“，”进行划分

   ```
   String[] columns = line.split(",");
   ```

3. 获取每行中对应位置，我需要的total_purchase_amt 和 total_redeem_amt数据

4. 如果对应位置的数据为empty，则将其赋值为0

   ```
   String reportDate = columns[1];
           String totalPurchaseAmt = columns[4].isEmpty() ? "0" : columns[4];
           String totalRedeemAmt = columns[8].isEmpty() ? "0" : columns[8];
   
   ```

5. 将其传给Reducer

   ```
    context.write(new Text(reportDate), new Text(totalPurchaseAmt + "," + totalRedeemAmt));
   ```

## Reducer

1. 对传入的数据进行划分处理

   ```
   String[] amounts = value.toString().split(",");
   ```

2. 统计相同日期下的流入和流出总量

   ```
   double inflow = Double.parseDouble(amounts[0]);
                       double outflow = Double.parseDouble(amounts[1]);
                       totalInflow += inflow;
                       totalOutflow += outflow;
   ```

3. 将结果输出，输出格式：<日期, "流入总量,流出总量">

   ```
   context.write(key, new Text((int) totalInflow + "," + (int) totalOutflow));
   ```

## 运行过程与结果

```
mvn clean package //打包jar

hadoop fs -put /home/czz/info/user_balance_table.csv /user/czz/input  //将文件传入输入路径

//运行程序，将结果存在lab2.1中
hadoop jar /lab2/target/hadoop_job-1.0-SNAPSHOT.jar com.example.jo1_Driver /user/czz/input/user_balance_table.csv /user/czz/output/lab2.1 
```

跑出最终结果，输出文件为lab2_job1_result。

跑出结果的格式如下：

```
20130701	32488348,5525022
20130702	29037390,2554548
20130703	27270770,5953867
20130704	18321185,6410729
20130705	11648749,2763587
20130706	36751272,1616635
20130707	8962232,3982735
20130708	57258266,8347729
20130709	26798941,3473059
20130710	30696506,2597169
20130711	44075197,3508800
20130712	34183904,8492573
......
```

# 任务二：星期交易量统计

## Mapper

### 1. 编写getWeekday（）函数，将日期字符串转化为星期

1）解析日期字符串

```
int year = Integer.parseInt(date.substring(0, 4));
int month = Integer.parseInt(date.substring(4, 6));
int day = Integer.parseInt(date.substring(6, 8));
```

2）计算是星期几

```
java.util.Calendar calendar = java.util.Calendar.getInstance();
calendar.set(year, month - 1, day);
int dayOfWeek = calendar.get(java.util.Calendar.DAY_OF_WEEK);
        
```

3）转化为星期几

```
switch (dayOfWeek) {
            case java.util.Calendar.SUNDAY: return "Sunday";
            case java.util.Calendar.MONDAY: return "Monday";
            case java.util.Calendar.TUESDAY: return "Tuesday";
            case java.util.Calendar.WEDNESDAY: return "Wednesday";
            case java.util.Calendar.THURSDAY: return "Thursday";
            case java.util.Calendar.FRIDAY: return "Friday";
            case java.util.Calendar.SATURDAY: return "Saturday";
            default: return null;
        }
```



### 2.解析输入文件，获取数据

1. 对传入文件进行处理，跳过第一行表头

2. 对每一行，依照tab键和“，”进行划分

   ```
   String[] parts = value.toString().split("\t");
   String date = parts[0];
   String[] amounts = parts[1].split(",");
   ```

3. 获取每行中对应位置，我需要的流入总量 和 流出总量数据

   ```
   String inflow = amounts[0];
   String outflow = amounts[1];
   
   // 解析日期，获取星期几
   String weekday = getWeekday(date);
   ```

4. 将其传给Reducer

   ```
   context.write(new Text(weekday), new Text(inflow + "," + outflow));
   ```

   

## Reducer

1. 统计相同星期的流入流出总额，并且计算总共的天数

   ```
   totalInflow += Long.parseLong(amounts[0]);
   totalOutflow += Long.parseLong(amounts[1]);
   count++;
   ```

2. 计算平均流入流出量，并输出结果

   ```
   long avgInflow = totalInflow / count;
   long avgOutflow = totalOutflow / count;
   context.write(key, new Text(avgInflow + "," + avgOutflow));
   ```

## 运行过程与结果

```
mvn clean package //打包jar


//运行程序，将结果存在lab2.2中
hadoop jar /lab2/target/hadoop_job-1.0-SNAPSHOT.jar com.example.jo2_Driver /user/czz/input/user_balance_table.csv /user/czz/output/lab2.2
```

跑出最终结果，输出文件为lab2_job2_result。

跑出结果的如下：

```
Friday	199407923,166467960
Monday	260305810,217463865
Saturday	148088068,112868942
Sunday	155914551,132427205
Thursday	236425594,176466674
Tuesday	263582058,191769144
Wednesday	254162607,194639446
```



# 任务三：用户活跃度分析

## Mapper

1. 跳过表头和无效数据行

   ```
   try {
                   Double.parseDouble(user_id); // 尝试将 user_id 转为数字
        } catch (NumberFormatException e) {
                   // 如果无法解析为数字，表明这是表头行或无效数据行，跳过
                   return;
        }
   ```

2. 获取direct_purchase_amt和total_redeem_amt数据

   ```
   double direct_purchase_amt = Double.parseDouble(fields[5]);
   double total_redeem_amt = Double.parseDouble(fields[8]);
   ```

3. 检查是否有直接购买或赎回行为

   ```
               if (direct_purchase_amt > 0 || total_redeem_amt > 0) {
                   userId.set(user_id);
                   context.write(userId, one);//private final static IntWritable one = new IntWritable(1);
               }
   ```

## Reducer

1. 累计活跃天数，并保存结果

   ```
           int activeDays = 0;
   
           // 累加活跃天数
           for (IntWritable val : values) {
               activeDays += val.get();
           }
           userActivityMap.put(key.toString(), activeDays);
   ```

2. 排序

   ```
   		// 将 Map 转换为 List 并按活跃天数降序排序
           List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(userActivityMap.entrySet());
           sortedList.sort((a, b) -> b.getValue().compareTo(a.getValue()));
   
           // 输出排序后的结果
           for (Map.Entry<String, Integer> entry : sortedList) {
               context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
           }
   ```

## 运行过程与结果

```
mvn clean package //打包jar


//运行程序，将结果存在lab2.3中
hadoop jar /lab2/target/hadoop_job-1.0-SNAPSHOT.jar com.example.jo3_Driver /user/czz/input/user_balance_table.csv /user/czz/output/lab2.3
```

跑出最终结果，输出文件为lab2_job3_result。

跑出结果，格式如下：

```
7629	384
11818	359
21723	334
19140	332
24378	315
26395	297
25147	295
27719	293
20515	291
......
```

# 任务四：交易行为影响因素分析

## 思路

1. 使用mfd_day_share_interest.csv中的数据和任务二中得到的结果分析
2. 统计mfd_day_share_interest.csv中，一周每天的平均interest信息
3. 根据统计出的interest信息和任务二中平均流入流出量，分析流入流出量和利率是否有关

## Mapper

### 1. 利用上面相同的getWeekday（）函数，将日期字符串转化为星期

1）解析日期字符串

```
int year = Integer.parseInt(date.substring(0, 4));
int month = Integer.parseInt(date.substring(4, 6));
int day = Integer.parseInt(date.substring(6, 8));
```

2）计算是星期几

```
java.util.Calendar calendar = java.util.Calendar.getInstance();
calendar.set(year, month - 1, day);
int dayOfWeek = calendar.get(java.util.Calendar.DAY_OF_WEEK);
        
```

3）转化为星期几

```
switch (dayOfWeek) {
            case java.util.Calendar.SUNDAY: return "Sunday";
            case java.util.Calendar.MONDAY: return "Monday";
            case java.util.Calendar.TUESDAY: return "Tuesday";
            case java.util.Calendar.WEDNESDAY: return "Wednesday";
            case java.util.Calendar.THURSDAY: return "Thursday";
            case java.util.Calendar.FRIDAY: return "Friday";
            case java.util.Calendar.SATURDAY: return "Saturday";
            default: return null;
        }
```



### 2.解析输入文件，获取数据

1. 对传入文件进行处理，跳过第一行表头

   ```
   		// 输入格式: <日期> TAB <流入总量>,<流出总量>
           if (firstLine) {
               firstLine = false; // 设置为 false，表示第一行已经跳过
               return;
           }
   ```

2. 对每一行，依照“，”进行划分

   ```
   String[] parts = value.toString().split(",");
   ```

3. 获取每行中对应位置，我需要的每日interest 和每周interest

   ```
   String rate = amounts[0];
   String rate7 = amounts[1];
   
   // 解析日期，获取星期几
   String weekday = getWeekday(date);
   ```

4. 将其传给Reducer

   ```
   context.write(new Text(weekday), new Text(rate + "," + rate7));
   ```

## Reducer

1. 统计相同星期的利率总数，并且计算总共的天数

   ```
   totalrate += Double.parseDouble(amounts[0]);
   totalrate7 += Double.parseDouble(amounts[1]);
   count++;
   ```

2. 计算平均利率，并输出结果

   ```
   double avgrate = totalrate / count;
   double avgrate7 = totalrate7 / count;
   context.write(key, new Text(avgrate + "," + avgrate7));
   ```

## 运行过程与结果

```
mvn clean package //打包jar

hadoop fs -put /home/czz/info/mfd_day_share_interest.csv /user/czz/input  //将文件传入输入路径

//运行程序，将结果存在lab2.4中
hadoop jar /lab2/target/hadoop_job-1.0-SNAPSHOT.jar com.example.jo4_Driver /user/czz/input/mfd_day_share_interest.csv /user/czz/output/lab2.4
```

跑出最终结果，输出文件为lab2_job4_result。

跑出结果的格式如下：

```
Friday	1.362986885245902,5.079016393442624
Monday	1.3635049180327876,5.100622950819672
Saturday	1.3412622950819677,5.074229508196722
Sunday	1.3413754098360653,5.06949180327869
Thursday	1.3527016393442621,5.084114754098361
Tuesday	1.3640114754098362,5.094557377049182
Wednesday	1.3543393442622953,5.089065573770491
```

## 分析结果

**相关性**：

- 平均利率和资金流入的相关系数为 0.80，与资金流出的相关系数为 0.84。这表明平均利率与资金流动间存在中等正相关。
- 七日年化收益率与资金流入的相关系数为 0.93，与资金流出的相关系数为 0.93，显示七日年化收益率与资金流动存在较强正相关关系。

**分析方法：**

使用 **皮尔逊相关系数**来衡量两组数据之间的线性相关性。皮尔逊相关系数的值在 -1 和 1 之间：

- **1** 表示完全正相关。
- **-1** 表示完全负相关。
- **0** 表示无线性相关性。

在 Python 中，使用 `scipy.stats` 模块的 `pearsonr` 函数可以计算出相关系数和对应的 p 值

**结果：**

七日年化收益率和平均利率都与资金流动呈显著正相关关系，尤其是七日年化收益率，表现出较强的解释能力。

# 遇到的问题

1. 在处理mapreduce传入两个文件时，出现无法识别路径的问题，无论是本地还是hdfs路径都无法识别

   解决办法：改用命令行传入地址的方法，而不是将其硬编码在代码中就可以识别了

   













