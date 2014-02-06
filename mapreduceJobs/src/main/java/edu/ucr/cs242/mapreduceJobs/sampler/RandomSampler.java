package edu.ucr.cs242.mapreduceJobs.sampler;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class RandomSampler {
    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "Sampler"); 
        job.setJarByClass(RandomSampler.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(RandomSamplerMapper.class);
        job.setReducerClass(RandomSamplingReducer.class);

        return job;
    }

    public static class RandomSamplerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private Random r = new Random(System.currentTimeMillis());
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int totalSize = context.getConfiguration().getInt("totalSize", 0);
            int sampleSize = context.getConfiguration().getInt("sampleSize", 0);
            int samplingRatio = (int) totalSize/ sampleSize;
            if (r.nextInt(samplingRatio) == 0)
                context.write(value, NullWritable.get());
        }
    }
    
    public static class RandomSamplingReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        
        @Override 
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}
