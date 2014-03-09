package edu.ucr.cs242.mapreduceJobs.hits.orig;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.ucr.cs242.mapreduceJobs.hits.orig.HitsOrigPreparation.HitsGroupingComparator;
import edu.ucr.cs242.mapreduceJobs.hits.orig.HitsOrigPreparation.HitsPartitioner;

public class HitsOrig {
    public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "HitsOrig");

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(HitsPartitioner.class);
        job.setGroupingComparatorClass(HitsGroupingComparator.class);
        job.setSortComparatorClass(HitsOrigComparator.class);
        job.setMapperClass(HitsOrigMapper.class);
        job.setReducerClass(HitsOrigReducer.class);

        return job;
    }

    public static class HitsOrigComparator extends WritableComparator {

        protected HitsOrigComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            Text t1 = (Text) o1;
            Text t2 = (Text) o2;
            String natKey1 = t1.toString().substring(t1.find(":") + 1);
            String natKey2 = t2.toString().substring(t2.find(":") + 1);
            int comp = natKey1.compareTo(natKey2);
            if (comp == 0)
                if (natKey1.indexOf("\t") > 0)
                    return -1;
                else if (natKey2.indexOf("\t") > 0)
                    return 1;
                else
                    return t1.toString().substring(0, t1.find(":")).compareTo(t2.toString().substring(0, t2.find(":")));
            return comp;
        }
    }

    public static class HitsOrigMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t", -1);
            Double hubScore = Double.valueOf(fields[0]);
            Double authScore = Double.valueOf(fields[1]);
            String[] pointToList = fields[2].split(",", -1);
            String[] pointedByList = fields[3].split(",", -1);

            for (String pointTo : pointToList)
                if (!pointTo.isEmpty())
                    context.write(new Text("a:" + pointTo), new Text(hubScore.toString()));
            for (String pointedBy : pointedByList)
                if (!pointedBy.isEmpty())
                    context.write(new Text("h:" + pointedBy), new Text(authScore.toString()));

            //emit graph links and previously calculated hub and auth scores
            context.write(new Text(value + ":" + key), value);
        }
    }

    public static class HitsOrigReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            Iterator<Text> valuesIt = values.iterator();
            if (!valuesIt.hasNext())
                return;
            String firstVal = valuesIt.next().toString();
            String[] firstValFields = firstVal.split("\t", -1);
            double prevHubScore = Double.parseDouble(firstValFields[0]);
            double prevAuthScore = Double.parseDouble(firstValFields[1]);
            String outLinks = firstValFields[2];
            String inLinks = firstValFields[3];

            double newHubScore = 0;
            double newAuthScore = 0;
            double score = 0;
            boolean changed = false;
            while (valuesIt.hasNext()) {
                Text value = valuesIt.next();
                if (key.toString().substring(0, key.find(":")).equals("h") && !changed) {
                    newAuthScore = score;
                    score = 0;
                    changed = true;
                }
                score += Double.parseDouble(value.toString());
            }
            if (changed)
                newHubScore = score;
            else
                newAuthScore = score;

            context.write(new Text("unnorm:" + key.toString().substring(key.find(":") + 1)), new Text(newHubScore
                    + "\t" + newAuthScore + "\t" + outLinks + "\t" + inLinks));
        }
    }
}
