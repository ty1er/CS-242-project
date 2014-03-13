package edu.ucr.cs242.mapreduceJobs.hits.orig;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HitsOrigPreparation {

    public static final int initialHubScore = 1;
    public static final int initialAuthorityScore = 1;

    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "HitsOrigPreparation");
        job.setJarByClass(HitsOrigPreparation.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(HitsPartitioner.class);
        job.setGroupingComparatorClass(HitsGroupingComparator.class);
        job.setSortComparatorClass(HitsComparator.class);
        job.setMapperClass(HitsPreparationMapper.class);
        job.setReducerClass(HitsPreparationReducer.class);

        return job;
    }

    public static class HitsComparator extends WritableComparator {

        protected HitsComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            Text t1 = (Text) o1;
            Text t2 = (Text) o2;
            String natKey1 = t1.toString().substring(t1.find(":") + 1);
            String natKey2 = t2.toString().substring(t2.find(":") + 1);
            int comp = natKey1.compareTo(natKey2);
            if (comp == 0) {
                String compKey1 = "";
                String compKey2 = "";
                if (t1.find(":") != -1)
                    compKey1 = t1.toString().substring(0, t1.find(":"));
                if (t2.find(":") != -1)
                    compKey2 = t2.toString().substring(0, t2.find(":"));
                return compKey1.compareTo(compKey2);
            }
            return comp;
        }

    }

    public static class HitsPreparationMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int separatorPos = value.find("\t");
            String retweetedUserId = value.toString().substring(0, separatorPos);
            String[] reweeters = value.toString().substring(separatorPos + 1).split(",");
            for (String retweeter : reweeters) {
                if (!retweeter.isEmpty()) {
                    context.write(new Text("h:" + retweetedUserId), new Text("h:" + retweeter));
                    context.write(new Text("a:" + retweeter), new Text("a:" + retweetedUserId));
                }
            }
        }
    }

    public static class HitsGroupingComparator extends WritableComparator {

        protected HitsGroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            Text t1 = (Text) o1;
            Text t2 = (Text) o2;
            if (t1 == null || t2 == null)
                return 0;
            return t1.toString().substring(t1.find(":") + 1).compareTo(t2.toString().substring(t2.find(":") + 1));
        }

    }

    public static final class HitsPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String newKey = key.toString().substring(key.find(":") + 1);
            return newKey.hashCode() % numPartitions;
        }
    }

    public static class HitsPreparationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {

            String pointedLinks = "";
            String links = "";
            boolean changed = false;
            for (Text value : values) {
                if (value.toString().substring(0, value.find(":")).equals("h") && !changed) {
                    pointedLinks = links;
                    links = "";
                    changed = true;
                }
                String trimmedVal = value.toString().substring(value.find(":") + 1);
                if (links.isEmpty())
                    links += trimmedVal;
                else
                    links += "," + trimmedVal;
            }
            String outLinks = changed ? links : "";
            String inLinks = changed ? pointedLinks : links;
            context.write(new Text(key.toString().substring(key.find(":") + 1)), new Text(initialHubScore + "\t"
                    + initialAuthorityScore + "\t" + outLinks + "\t" + inLinks));
        }
    }
}
