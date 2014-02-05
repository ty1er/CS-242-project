package edu.ucr.cs242.mapreduceJobs.sampler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Analytics {
    public static Job createFollowerCounterJob() throws IOException {
        Job job = new Job(new Configuration(), "Analytics"); 
        job.setJarByClass(Analytics.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(KeyCounterMapper.class);
        job.setReducerClass(CounterReducer.class);

        return job;
    }
    
    public static Job createFollowerHistorgammJob() throws IOException {
        Job job = new Job(new Configuration(), "Histogramm"); 
        job.setJarByClass(Analytics.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(ValueCounterMapper.class);
        job.setReducerClass(CounterReducer.class);

        return job;
    }

    public static class KeyCounterMapper extends Mapper<Text, Text, LongWritable, LongWritable> {        
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Long longKey = Long.parseLong(key.toString());
            context.write(new LongWritable(longKey),new LongWritable(1L));
        }
    }
    
    public static class CounterReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        
        @Override 
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values)
                sum = sum + value.get();
            context.write(key, new LongWritable(sum));
        }
    }
    
    public static class ValueCounterMapper extends Mapper<Text, Text, LongWritable, LongWritable> {        
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Long longVal = Long.parseLong(value.toString());
            context.write(new LongWritable(longVal),new LongWritable(1L));
        }
    }
}
