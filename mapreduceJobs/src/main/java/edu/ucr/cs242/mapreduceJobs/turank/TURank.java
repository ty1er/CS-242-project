package edu.ucr.cs242.mapreduceJobs.turank;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.log4j.Logger;

import edu.ucr.cs242.mapreduceJobs.turank.TURankMain.IterationCounter;

public class TURank {

    private static Logger log = Logger.getLogger(TURank.class);

    public static double followWeight = 0.2;
    public static double rtWeight = 0.4;
    public static double postWeight = 0.8;
    public static double postedWeight = 0.6;

    public static final double dampingFactor = 0.85;

    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "TURank");

        job.setJarByClass(TURank.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(TURankComparator.class);
        job.setGroupingComparatorClass(TURankGroupingComparator.class);
        job.setPartitionerClass(TURankPartitioner.class);
        job.setMapperClass(TURankMapper.class);
        job.setReducerClass(TURankReducer.class);

        return job;
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
            return t1.toString().substring(0, t1.find(":")).compareTo(t2.toString().substring(0, t2.find(":")));
        }

    }

    public static class TURankComparator extends WritableComparator {

        protected TURankComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            Text t1 = (Text) o1;
            Text t2 = (Text) o2;
            String natKey1 = t1.toString().substring(0, t1.find(":"));
            String natKey2 = t2.toString().substring(0, t2.find(":"));
            int comp = natKey1.compareTo(natKey2);
            if (comp == 0) {
                if (t1.toString().substring(t1.find(":") + 1).indexOf("\t") > 0)
                    return -1;
                else if (t2.toString().substring(t2.find(":") + 1).indexOf("\t") > 0)
                    return 1;
                else
                    return t1.toString().substring(t1.find(":") + 1)
                            .compareTo(t2.toString().substring(t2.find(":") + 1));
            }
            return comp;
        }

    }

    public static final class TURankPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String newKey = key.toString().substring(0, key.find(":"));
            return newKey.hashCode() % numPartitions;
        }
    }

    public static class TURankMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t", -1);
            double currentTR = Double.parseDouble(fields[0]);
            Long outFollowLinksNum = Long.parseLong(fields[1]);
            String[] followers = fields[2].split(",");
            Long outRTLinksNum = Long.parseLong(fields[3]);
            String[] RTs = fields[4].split(",");
            Long outPostLinksNum = Long.parseLong(fields[5]);
            String[] posts = fields[6].split(",");
            String posted = fields[7];

            for (String follower : followers)
                if (outFollowLinksNum != 0)
                    context.write(new Text(follower + ":"
                            + (currentTR * followWeight * dampingFactor / outFollowLinksNum)), new Text(
                            ((Double) (currentTR * followWeight * dampingFactor / outFollowLinksNum)).toString()));
            for (String rt : RTs)
                if (outRTLinksNum != 0)
                    context.write(new Text(rt + ":" + (currentTR * followWeight * dampingFactor / outRTLinksNum)),
                            new Text(((Double) (currentTR * followWeight * dampingFactor / outRTLinksNum)).toString()));
            for (String post : posts)
                if (outPostLinksNum != 0)
                    context.write(new Text(post + ":" + (currentTR * postWeight * dampingFactor / outPostLinksNum)),
                            new Text(((Double) (currentTR * postWeight * dampingFactor / outPostLinksNum)).toString()));

            if (!posted.equals("0") && !posted.isEmpty())
                context.write(new Text(posted + ":" + (currentTR * postedWeight * dampingFactor)), new Text(
                        ((Double) (currentTR * postedWeight * dampingFactor)).toString()));

            //ensure that reducer will be executed for each key  
            context.write(new Text(key + ":0"), new Text("0"));

            //emit graph links and previously calculated pageRank
            context.write(new Text(key + ":" + value), value);
        }
    }

    public static class TURankReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            Iterator<Text> valuesIt = values.iterator();
            if (!valuesIt.hasNext())
                return;
            String firstVal = valuesIt.next().toString();
            int delimiterPos = firstVal.toString().indexOf('\t');
            double prevRP = 0;
            try {
                prevRP = Double.parseDouble(firstVal.substring(0, delimiterPos));
            } catch (Exception e) {
                log.error("Error during processing of first value \"" + firstVal + "\":" + e.getMessage(), e);
            }
            String otherFields = firstVal.substring(delimiterPos + 1);

            //calculating new pageRank
            double newPR = 0;
            while (valuesIt.hasNext()) {
                String value = valuesIt.next().toString();
                newPR += Double.parseDouble(value);
            }
            newPR = newPR + (1 - dampingFactor);

            //deciding whether another iteration is needed
            context.getCounter(IterationCounter.RESIDUAL).increment(
                    (Long) Math.round(Math.abs(prevRP - newPR) * TURankMain.counterReciprocal));

            //output
            context.write(new Text(key.toString().substring(0, key.find(":"))), new Text(newPR + "\t" + otherFields));
        }
    }
}
