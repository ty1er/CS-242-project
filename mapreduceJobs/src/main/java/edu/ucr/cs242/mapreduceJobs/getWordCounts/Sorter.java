package edu.ucr.cs242.mapreduceJobs.getWordCounts;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.ucr.cs242.mapreduceJobs.getWordCounts.WordCounts.PageRankPartitioner;
import edu.ucr.cs242.mapreduceJobs.getWordCounts.WordCounts.WordCountCombiner;
import edu.ucr.cs242.mapreduceJobs.getWordCounts.WordCounts.WordCountMapper;
import edu.ucr.cs242.mapreduceJobs.getWordCounts.WordCounts.WordCountReducer;
import edu.ucr.cs242.mapreduceJobs.pagerank.PagePank;

public class Sorter {

	private static Logger log = Logger.getLogger(PagePank.class);
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

	public static final class PageRankPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString();
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class SorterMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
					context.write(value, key);

		}
	}
	

	public static class SorterReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();
			if (!valuesIt.hasNext())
				return;
			while (valuesIt.hasNext()) {
				context.write(key, new Text(valuesIt.next().toString()));
			}
		}
	}
}
