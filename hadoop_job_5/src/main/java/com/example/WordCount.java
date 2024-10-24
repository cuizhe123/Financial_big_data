package com.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
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

import com.example.WordCount.IntSumReducer.WordCountEntry;
import com.example.WordCount.TokenizerMapper;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\W+"); // 匹配非字母数字字符

        // setup 方法读取停词表
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopWordsPath = conf.get("stopwords.path"); // 停词文件路径
            BufferedReader reader = new BufferedReader(new FileReader(stopWordsPath));
            String line;
            while ((line = reader.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase()); // 加入停词表
            }
            reader.close();
        }
        private boolean isFirstLine = true; // 用于标记是否是第一行
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (isFirstLine) {
                isFirstLine = false; // 设置为 false，以后不再跳过
                return; // 直接返回，跳过这一行
            }
            String[] fields = value.toString().split(",");
            // 确保有足够的字段
            if (fields.length > 1) {
                for (int i = 1; i < fields.length; i++) {
                    String headline = fields[i].toLowerCase(); // 将新闻标题转为小写
                    String[] tokens = headline.split("\\W+"); // 使用非单词字符分割

                    for (String token : tokens) {
                        if (token.length() > 1 && !token.matches(".*\\d.*") && !stopWords.contains(token)) {
                            word.set(token);
                            context.write(word, one);
                        }
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private List<WordCountEntry> wordCountList = new ArrayList<>();

        public static class WordCountEntry {
            String word;
            int count;

            public WordCountEntry(String word, int count) {
                this.word = word;
                this.count = count;
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            wordCountList.add(new WordCountEntry(key.toString(), sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 按次数排序
            Collections.sort(wordCountList, new Comparator<WordCountEntry>() {
                @Override
                public int compare(WordCountEntry o1, WordCountEntry o2) {
                    return Integer.compare(o2.count, o1.count); // 从大到小排序
                }
            });

            // 输出前100个高频词
            
            for (int i = 0; i < Math.min(100, wordCountList.size()); i++) {
                WordCountEntry entry = wordCountList.get(i);
                context.write(new Text((i+1)+ ":"+entry.word), new IntWritable(entry.count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 将停词表路径传递给作业
        conf.set("stopwords.path", args[2]); 

        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
