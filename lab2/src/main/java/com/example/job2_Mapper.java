package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class job2_Mapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 输入格式: <日期> TAB <流入总量>,<流出总量>
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            String date = parts[0];
            String[] amounts = parts[1].split(",");
            if (amounts.length == 2) {
                String inflow = amounts[0];
                String outflow = amounts[1];

                // 解析日期，获取星期几
                String weekday = getWeekday(date);
                context.write(new Text(weekday), new Text(inflow + "," + outflow));
            }
        }
    }

    private String getWeekday(String date) {
        // 解析日期字符串（假设格式为 YYYYMMDD）
        int year = Integer.parseInt(date.substring(0, 4));
        int month = Integer.parseInt(date.substring(4, 6));
        int day = Integer.parseInt(date.substring(6, 8));
        
        // 计算星期几
        java.util.Calendar calendar = java.util.Calendar.getInstance();
        calendar.set(year, month - 1, day);
        int dayOfWeek = calendar.get(java.util.Calendar.DAY_OF_WEEK);
        
        // 将数字转换为星期几名称
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
    }
}
