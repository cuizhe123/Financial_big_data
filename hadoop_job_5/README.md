# StockCount

## map

1. 设置标记位，跳过csv文件的第一行，从第二位开始读取
2. 依照“，”对字符串进行划分，只使用length-1位的字符串，对应的为Stock的值。

## reduce

1. 将相同Stock的数目进行相加
2. 对其按照从大到小进行排序，输出格式为<排名>：<股票代码>，<次数>

WEB：

![image-20241023201019907](C:\Users\Annian\AppData\Roaming\Typora\typora-user-images\image-20241023201019907.png)

![image-20241024121108429](C:\Users\Annian\AppData\Roaming\Typora\typora-user-images\image-20241024121108429.png)



# WordCount

## map

1. 使用setup方法读取停词表
2. 设置标记位，跳过csv文件的第一行，从第二位开始读取
3. 依照“，”对字符串进行划分，将fields[1] 到fields[length-1]的内容全部计入，防止因提前出现“，”导致少计入词
4. 将每一个词都化为小写，同时去除其中只有一个字母的单词和含有数字的单词。

## reduce

1. 将相同Stock的数目进行相加

2. 对其按照从大到小进行排序

3. 输出前一百的高频词，格式为<排名>：<词>，<次数>

   



wordcount：

![image-20241023194725942](C:\Users\Annian\AppData\Roaming\Typora\typora-user-images\image-20241023194725942.png)

运行wordcount：

![image-20241023200949347](C:\Users\Annian\AppData\Roaming\Typora\typora-user-images\image-20241023200949347.png)



代码和输出结果请详见其他文件



















































问题：

1. 不能依照‘，’来进行分割字符串，不然会出问题

![image-20241023151050849](C:\Users\Annian\AppData\Roaming\Typora\typora-user-images\image-20241023151050849.png)

更改：
