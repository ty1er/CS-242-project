package edu.ucr.cs242.mapreduceJobs.turank;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.base.Joiner;

public class TURankPreparation {

    public static final double initialTR = 1.0;

    public static Job createJob(Path tweetInput, Path retweetPath, Path usersPath) throws IOException {
        Job job = new Job(new Configuration(), "HitsOrigPreparation");

        job.setJarByClass(TURankPreparation.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(TURankPartitioner.class);
        job.setGroupingComparatorClass(TURankGroupingComparator.class);
        job.setSortComparatorClass(TURankComparator.class);
        MultipleInputs.addInputPath(job, tweetInput, KeyValueTextInputFormat.class, TURankTweetMapper.class);
        MultipleInputs.addInputPath(job, retweetPath, KeyValueTextInputFormat.class, TURankRetweetMapper.class);
        MultipleInputs.addInputPath(job, usersPath, KeyValueTextInputFormat.class, TURankUsersMapper.class);
        //        job.setMapperClass(HitsPreparationMapper.class);
        job.setReducerClass(TURankPreparationReducer.class);

        return job;
    }

    public static class TURankComparator extends WritableComparator {

        protected TURankComparator() {
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

    //process social graph
    public static class TURankUsersMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("follow:user_" + value), new Text("user_" + key));
            context.write(new Text("follow:user_" + key.toString()), new Text(""));
        }
    }

    //process retweet log
    public static class TURankRetweetMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int tabSeparatorPos = value.find("\t");
            String retweetedUserId = value.toString().substring(0, tabSeparatorPos);
            String[] reweeters = value.toString().substring(tabSeparatorPos + 1).split(",");
            context.write(new Text("post:" + "user_" + retweetedUserId), new Text("tweet_" + key));
            context.write(new Text("posted:" + "tweet_" + key), new Text("user_" + retweetedUserId));
            for (String retweeter : reweeters) {
                if (!retweeter.isEmpty()) {
                    context.write(new Text("rt:" + "user_" + retweeter), new Text("tweet_" + key.toString()));
                }
            }

        }
    }

    //process tweet log
    public static class TURankTweetMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int tabSeparatorPos = value.find("\t");
            String tweetId = value.toString().substring(0, tabSeparatorPos);
            context.write(new Text("post:" + "user_" + key), new Text("tweet_" + tweetId));
            context.write(new Text("posted:" + "tweet_" + tweetId), new Text("user_" + key));
        }
    }

    public static class TURankGroupingComparator extends WritableComparator {

        protected TURankGroupingComparator() {
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

    public static final class TURankPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String newKey = key.toString().substring(key.find(":") + 1);
            return newKey.hashCode() % numPartitions;
        }
    }

    public static class TURankPreparationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {

            Map<String, List<String>> links = new HashMap<String, List<String>>();
            links.put("follow", new LinkedList<String>());
            links.put("post", new LinkedList<String>());
            links.put("rt", new LinkedList<String>());
            links.put("posted", new LinkedList<String>());
            for (Text value : values) {
                if (!value.toString().isEmpty())
                    links.get(key.toString().substring(0, key.find(":"))).add(value.toString());
            }
            StringBuilder sb = new StringBuilder();
            sb.append(initialTR).append("\t").append(links.get("follow").size()).append("\t")
                    .append(Joiner.on(',').join(links.get("follow")));
            sb.append("\t").append(links.get("rt").size()).append("\t").append(Joiner.on(',').join(links.get("rt")));
            sb.append("\t").append(links.get("post").size()).append("\t")
                    .append(Joiner.on(',').join(links.get("post")));
            if (links.get("posted").size() > 0)
                sb.append("\t").append(links.get("posted").get(0));
            else
                sb.append("\t").append(0);
            String outKey = key.toString().substring(key.find(":") + 1);
            context.write(new Text(outKey), new Text(sb.toString()));
        }
    }
}
