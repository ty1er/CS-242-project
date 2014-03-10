package edu.ucr.cs242.mapreduceJobs.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.index.IndexWriter;

import edu.ucr.cs242.mapreduceJobs.idf.IDF.IDFComparator;
import edu.ucr.cs242.mapreduceJobs.idf.IDF.IDFGroupingComparator;
import edu.ucr.cs242.mapreduceJobs.idf.IDF.IDFMapper;
import edu.ucr.cs242.mapreduceJobs.idf.IDF.IDFPartitioner;
import edu.ucr.cs242.mapreduceJobs.idf.IDF.IDFReducer;

public class TFIDF {

	public static Job createJob() throws IOException {
        Job job = new Job(new Configuration(), "TFIDF");
        job.setJarByClass(TFIDF.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(IDFComparator.class);
        job.setGroupingComparatorClass(IDFGroupingComparator.class);
        job.setPartitionerClass(IDFPartitioner.class);
        job.setMapperClass(IDFMapper.class);
        job.setReducerClass(IDFReducer.class);


		return job;
	}
    public static class IDFComparator extends WritableComparator {

        protected IDFComparator() {
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
	
	

	public static final class IDFPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString().substring(0, key.find(":"));
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class IDFGroupingComparator extends WritableComparator {

		protected IDFGroupingComparator() {
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

	public static class IDFMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			context.write(key, value);

		}
	}

	public static class IDFReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();

			double N = 10326522;
			double df = 0;

			if (!valuesIt.hasNext())
				return;

			while (valuesIt.hasNext()) {
				String value = valuesIt.next().toString();
				String[] pieces = value.split("\\:");
				df++;
			}
			if (df > 1000) {
				double idf = Math.log(N / df);
				context.write(key, new Text(String.valueOf(idf)));
			}
		}
	}
}
