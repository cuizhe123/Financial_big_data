package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.StockCount.StockCountEntry;
import com.example.StockCount.StockMapper;
import com.example.StockCount.StockReducer;

public class StockCount {

    // 自定义类来存储股票代码和计数
    public static class StockCountEntry {
        String stockCode;
        int count;

        public StockCountEntry(String stockCode, int count) {
            this.stockCode = stockCode;
            this.count = count;
        }
    }

    public static class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text stock = new Text();
        private boolean isFirstLine = true; // 用于标记是否是第一行

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isFirstLine) {
                isFirstLine = false; // 设置为 false，以后不再跳过
                return; // 直接返回，跳过这一行
            }
            String[] fields = value.toString().split(",");
            if (fields.length > 3) {
                stock.set(fields[fields.length - 1]); // 获取股票代码
                context.write(stock, one);
            }
        }
    }

    public static class StockReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private List<StockCountEntry> stockCounts = new ArrayList<>();
    
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            stockCounts.add(new StockCountEntry(key.toString(), sum));
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 按出现次数排序
            Collections.sort(stockCounts, new Comparator<StockCountEntry>() {
                @Override
                public int compare(StockCountEntry o1, StockCountEntry o2) {
                    return Integer.compare(o2.count, o1.count); // 从大到小排序
                }
            });
    
            // 输出结果，格式为 "<排名>：<股票代码>，<次数>"
            for (int rank = 0; rank < stockCounts.size(); rank++) {
                StockCountEntry entry = stockCounts.get(rank);
                // 使用 String.format 来构建输出结果
                String result = String.format("%d: %s", rank+1, entry.stockCode);
                // String result = String.format("%d", rank+1);
                // String result = String.format("%s", entry.stockCode);
                context.write(new Text(result), new IntWritable(entry.count));  
            }
        }
    }
    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock Count");
        job.setJarByClass(StockCount.class);
        job.setMapperClass(StockMapper.class);
        // job.setCombinerClass(StockReducer.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
