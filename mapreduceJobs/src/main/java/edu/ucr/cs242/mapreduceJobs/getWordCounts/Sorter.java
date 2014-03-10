package edu.ucr.cs242.mapreduceJobs.getWordCounts;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

public class Sorter {

    private static Logger log = Logger.getLogger(Sorter.class);
    public static final double dampingFactor = 0.85;

    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "Sorter");
        job.setJarByClass(Sorter.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(PageRankPartitioner.class);

        job.setMapperClass(SorterMapper.class);
        job.setReducerClass(SorterReducer.class);

        return job;
    }

    public static final class PageRankPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String newKey = key.toString();
            return newKey.hashCode() % numPartitions;
        }
    }

    public static class SorterMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, key);

        }
    }

    public static class SorterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            Iterator<Text> valuesIt = values.iterator();
            if (!valuesIt.hasNext())
                return;
            while (valuesIt.hasNext()) {
                context.write(key, new Text(valuesIt.next().toString()));
            }
        }
    }
}
