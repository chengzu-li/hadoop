package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class digit_sorting extends Configured {
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        private IntWritable mapOutputkey = new IntWritable();
        private final static IntWritable mapOutputValue = new IntWritable(1);

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            //删去头尾空白符
            StringTokenizer strtoken = new StringTokenizer(line);
            //tokenize
            while(strtoken.hasMoreTokens()){
                String word = strtoken.nextToken();
                int number = Integer.parseInt(word);
                mapOutputkey.set(number);
                context.write(mapOutputkey, mapOutputValue);
            }
        }
    }

    public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable reduceOutputValue = new IntWritable(1);

        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int num = 0;
            for(IntWritable value:values){
                num = num + value.get();
            }
            reduceOutputValue.set(num);
            context.write(key, reduceOutputValue);
        }
    }

    public static void main(String[] args)throws Exception{
        args = new String[] {"hdfs://192.168.186.100:9000/hdfs_file/test/digit_test.txt",
                "hdfs://192.168.186.100:9000/hdfs_file/test/my_digit_test_output"};

        Configuration conf = new Configuration();
        conf.set("dfs.defaultFS", "192.168.186.100:9000");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "192.168.186.100");
        conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf);
        job.setJarByClass(digit_sorting.class);

        job.setMapperClass(digit_sorting.MyMapper.class);
        job.setCombinerClass(digit_sorting.MyReducer.class);
        job.setReducerClass(digit_sorting.MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);

        System.out.println(status);
    }
}
