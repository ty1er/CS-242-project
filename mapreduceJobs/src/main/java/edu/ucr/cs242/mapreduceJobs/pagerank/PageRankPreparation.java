package edu.ucr.cs242.mapreduceJobs.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRankPreparation {

    public static final double initialPR = 1.0;

    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "PageRankPreparation");
        job.setJarByClass(PageRankPreparation.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PagePankPreparationMapper.class);
        job.setReducerClass(PagePankPreparationReducer.class);

        return job;
    }

    public static class PagePankPreparationMapper extends Mapper<Text, Text, LongWritable, LongWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(Long.parseLong(value.toString())),
                    new LongWritable(Long.parseLong(key.toString())));
            context.write(new LongWritable(Long.parseLong(key.toString())), new LongWritable(-1));
        }
    }

    public static class PagePankPreparationReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException,
                InterruptedException {
            int outLinksNum = 0;
            String outLinks = "";
            Iterator<LongWritable> it = values.iterator();
            if (it.hasNext()) {
                LongWritable value = it.next();
                while (it.hasNext() && value.get() == -1) {
                    value = it.next();
                }
                if (value.get() != -1) {
                    outLinksNum++;
                    outLinks += value;
                }
            }
            while (it.hasNext()) {
                LongWritable value = it.next();
                if (value.get() != -1) {
                    outLinksNum++;
                    outLinks += "," + value.toString();
                }
            }
            context.write(key, new Text(initialPR + "\t" + outLinksNum + "\t" + outLinks));
        }
    }
}
