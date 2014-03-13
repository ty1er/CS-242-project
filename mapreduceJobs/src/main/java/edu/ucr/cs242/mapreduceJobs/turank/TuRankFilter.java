package edu.ucr.cs242.mapreduceJobs.turank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TuRankFilter {

    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "TuRankFilter");
        job.setJarByClass(TuRankFilter.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(TuRankFilterMapper.class);
        job.setReducerClass(TuRankFilterReducer.class);

        return job;
    }

    public static class TuRankFilterMapper extends Mapper<Text, Text, LongWritable, DoubleWritable> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            if (key.toString().startsWith("tweet_")) {
                long tid = Long.parseLong(key.toString().substring(key.find("_") + 1));
                double score = Double.parseDouble(value.toString().substring(0, value.find("\t")));
                context.write(new LongWritable(tid), new DoubleWritable(score));
            }
        }
    }

    public static class TuRankFilterReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
    }
}
