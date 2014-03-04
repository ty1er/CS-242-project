package edu.ucr.cs242.mapreduceJobs.pagerank;

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

import edu.ucr.cs242.mapreduceJobs.pagerank.PageRankMain.IterationCounter;

public class PagePank {

    private static Logger log = Logger.getLogger(PagePank.class);

    public static final double dampingFactor = 0.85;

    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "PageRank");

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(PageRankComparator.class);
        job.setGroupingComparatorClass(PargeRankGroupingComparator.class);
        job.setPartitionerClass(PageRankPartitioner.class);
        job.setMapperClass(PagePankMapper.class);
        job.setReducerClass(PagePankReducer.class);

        return job;
    }

    public static class PargeRankGroupingComparator extends WritableComparator {

        protected PargeRankGroupingComparator() {
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

    public static class PageRankComparator extends WritableComparator {

        protected PageRankComparator() {
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

    public static final class PageRankPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String newKey = key.toString().substring(0, key.find(":"));
            return newKey.hashCode() % numPartitions;
        }
    }

    public static class PagePankMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int firstDelimiterPos = value.toString().indexOf('\t');
            int lastDelimiterPos = value.toString().lastIndexOf('\t');
            double currentPR = Double.parseDouble(value.toString().substring(0, firstDelimiterPos));
            Long outLinksNum = Long.parseLong(value.toString().substring(firstDelimiterPos + 1, lastDelimiterPos));
            String followers = value.toString().substring(lastDelimiterPos + 1);

            for (String follower : followers.split(","))
                if (outLinksNum != 0)
                    context.write(new Text(follower + ":" + (currentPR * dampingFactor / outLinksNum)), new Text(
                            ((Double) (currentPR * dampingFactor / outLinksNum)).toString()));

            //ensure that reducer will be executed for each key  
            context.write(new Text(key + ":0"), new Text("0"));

            //emit graph links and previously calculated pageRank
            context.write(new Text(key + ":" + value), value);
        }
    }

    public static class PagePankReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            Iterator<Text> valuesIt = values.iterator();
            if (!valuesIt.hasNext())
                return;
            String firstVal = valuesIt.next().toString();
            int firstDelimiterPos = firstVal.toString().indexOf('\t');
            int lastDelimiterPos = firstVal.toString().lastIndexOf('\t');
            double prevRP = Double.parseDouble(firstVal.substring(0, firstDelimiterPos));
            long outLinksNum = Long.parseLong(firstVal.substring(firstDelimiterPos + 1, lastDelimiterPos));
            String links = firstVal.substring(lastDelimiterPos + 1);

            //calculating new pageRank
            double newPR = 0;
            while (valuesIt.hasNext()) {
                String value = valuesIt.next().toString();
                newPR += Double.parseDouble(value);
            }
            newPR = newPR + (1 - dampingFactor);

            //deciding whether another iteration is needed
            context.getCounter(IterationCounter.RESIDUAL).increment(
                    (Long) Math.round(Math.abs(prevRP - newPR) * PageRankMain.counterReciprocal));

            //output
            context.write(new Text(key.toString().substring(0, key.find(":"))), new Text(newPR + "\t" + outLinksNum
                    + "\t" + links));
        }
    }
}
