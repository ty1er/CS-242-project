package edu.ucr.cs242.mapreduceJobs.scorecombiner;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.ucr.cs242.mapreduceJobs.scorecombiner.Sorter.SorterGroupingComparator;
import edu.ucr.cs242.mapreduceJobs.scorecombiner.Sorter.SorterMapper;
import edu.ucr.cs242.mapreduceJobs.scorecombiner.Sorter.SorterPartitioner;
import edu.ucr.cs242.mapreduceJobs.scorecombiner.Sorter.SorterReducer;

public class Sorter {

	public static Job createJob() throws IOException {
		Job job = new Job(new Configuration(), "Sorter");
		job.setJarByClass(Sorter.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setGroupingComparatorClass(SorterGroupingComparator.class);
		job.setPartitionerClass(SorterPartitioner.class);
		job.setMapperClass(SorterMapper.class);
		job.setReducerClass(SorterReducer.class);

		return job;
	}

	public static final class SorterPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString();
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class SorterGroupingComparator extends WritableComparator {

		protected SorterGroupingComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			Text t1 = (Text) o1;
			Text t2 = (Text) o2;
			Double a = Double.parseDouble(t1.toString());
			Double b = Double.parseDouble(t2.toString());
			if (t1 == null || t2 == null)
				return 0;
			return a.compareTo(b);
			
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
			
			String value = valuesIt.next().toString();
			context.write(key, new Text(value));

		}
	}
}
