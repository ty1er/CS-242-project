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

public class ScreenNameFilter {
    public static Job createJoinJob() throws IOException {
        Job job = new Job(new Configuration(), "ScreenNameJoin"); 
        job.setJarByClass(Analytics.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        return job;
    }

    public static class JoinMapper extends Mapper<Text, Text, LongWritable, Text> {        
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Long longKey = Long.parseLong(key.toString());
            context.write(new LongWritable(longKey), value);
        }
    }
    
    public static class JoinReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        
        @Override 
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            String screenName = "";
            for (Text value : values)
            {
                i++;
                try {
                    Long.parseLong(value.toString());
                }
                catch(NumberFormatException nfe)
                {
                    screenName = value.toString();
                }
            }
            if (i > 1)
                context.write(key, new Text(screenName));
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
