package edu.ucr.cs242.mapreduceJobs.hits.orig;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.ucr.cs242.mapreduceJobs.hits.orig.HitsMain.IterationCounter;
import edu.ucr.cs242.mapreduceJobs.hits.orig.HitsOrigPreparation.HitsComparator;
import edu.ucr.cs242.mapreduceJobs.hits.orig.HitsOrigPreparation.HitsGroupingComparator;
import edu.ucr.cs242.mapreduceJobs.hits.orig.HitsOrigPreparation.HitsPartitioner;

public class HitsNormalization {
    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "HitsNormalize");

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(HitsPartitioner.class);
        job.setGroupingComparatorClass(HitsGroupingComparator.class);
        job.setSortComparatorClass(HitsComparator.class);
        job.setMapperClass(HitsNormalizeMapper.class);
        job.setReducerClass(HitsNormalizeReducer.class);

        return job;
    }

    public static class HitsNormalizeMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t", -1);
            double hubScore = Double.valueOf(fields[0]);
            double authScore = Double.valueOf(fields[1]);
            String[] outLinks = fields[2].split(",", -1);
            String[] inLinks = fields[3].split(",", -1);

            double authNorm = context.getConfiguration().getDouble("authNorm", 0);
            double hubNorm = context.getConfiguration().getDouble("hubNorm", 0);
            authScore /= authNorm;
            hubScore /= hubNorm;

            if (key.toString().startsWith("unnorm:"))
                context.write(key, new Text(hubScore + "\t" + authScore + "\t" + outLinks + "\t" + inLinks));
            else
                context.write(key, new Text(value));
        }
    }

    public static class HitsNormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {

            Iterator<Text> valuesIt = values.iterator();

            String oldVal = valuesIt.next().toString();
            String newVal = valuesIt.next().toString();
            if (valuesIt.hasNext())
                throw new IOException("Unexpected value");

            String[] oldValFields = oldVal.split("\t", -1);
            String[] newValFields = newVal.split("\t", -1);
            double prevHubScore = Double.parseDouble(oldValFields[0]);
            double prevAuthScore = Double.parseDouble(oldValFields[1]);
            double newHubScore = Double.parseDouble(newValFields[0]);
            double newAuthScore = Double.parseDouble(newValFields[1]);
            String outLinks = oldValFields[2];
            String inLinks = oldValFields[3];

            //deciding whether another iteration is needed
            context.getCounter(IterationCounter.RESIDUAL).increment(
                    (Long) (Math.round((Math.abs(newHubScore - prevHubScore) + Math.abs(newAuthScore - prevAuthScore))
                            * HitsMain.counterReciprocal)));

            context.write(new Text(key.toString().substring(key.find(":") + 1)), new Text(newHubScore + "\t"
                    + newAuthScore + "\t" + outLinks + "\t" + inLinks));
        }
    }
}
