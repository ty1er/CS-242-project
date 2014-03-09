package edu.ucr.cs242.mapreduceJobs.hits.orig;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class HitsNormCalc {
    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "HitsNormCalc");

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(HitsNormalizeMapper.class);
        job.setReducerClass(HitsNormalizeReducer.class);

        return job;
    }

    public static class HitsNormalizeMapper extends Mapper<Text, Text, Text, DoubleWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t", -1);
            Double hubScore = Double.parseDouble(fields[0]);
            Double authScore = Double.parseDouble(fields[1]);

            context.write(new Text("auth"), new DoubleWritable(authScore));
            context.write(new Text("hub"), new DoubleWritable(hubScore));
        }
    }

    public static class HitsNormalizeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,
                InterruptedException {

            double normalization = 0;
            for (DoubleWritable value : values)
                normalization += Math.pow(value.get(), 2.0);
            normalization = Math.sqrt(normalization);
            context.write(key, new DoubleWritable(normalization));
        }
    }
}
