package edu.ucr.cs242.mapreduceJobs.idf;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.index.IndexWriter;

import edu.ucr.cs242.mapreduceJobs.pagerank.PageRank.PagePankMapper;
import edu.ucr.cs242.mapreduceJobs.pagerank.PageRank.PagePankReducer;
import edu.ucr.cs242.mapreduceJobs.pagerank.PageRank.PageRankComparator;
import edu.ucr.cs242.mapreduceJobs.pagerank.PageRank.PageRankPartitioner;
import edu.ucr.cs242.mapreduceJobs.pagerank.PageRank.PargeRankGroupingComparator;

public class IDF {

	public static Job createJob() throws IOException {
		Job job = new Job(new Configuration(), "IDF");
		job.setJarByClass(IDF.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(IDFGroupingComparator.class);
        job.setPartitionerClass(IDFPartitioner.class);
        job.setMapperClass(IDFMapper.class);
        job.setReducerClass(IDFReducer.class);


		return job;
	}
	
	

	public static final class IDFPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString();
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
			return t1.toString().compareTo(t2.toString());
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
				df++;
			}
			if (df >= 1000) {
				double idf = Math.log(N / df);
				context.write(key, new Text(String.valueOf(idf)));
			}
		}
	}
}
