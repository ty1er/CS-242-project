package edu.ucr.cs242.mapreduceJobs.saveTweets;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SaveTweets {

	public static Job createJob() throws IOException {
		Job job = new Job(new Configuration(), "SaveTweets");
		job.setJarByClass(SaveTweets.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setGroupingComparatorClass(SaveTweetsGroupingComparator.class);
		job.setPartitionerClass(SaveTweetsPartitioner.class);
		job.setMapperClass(SaveTweetsMapper.class);
		job.setReducerClass(SaveTweetsReducer.class);
		job.setNumReduceTasks(1000);

		return job;
	}

	public static final class SaveTweetsPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString();
			if (newKey.length() > 5){
				return newKey.substring(0, 5).hashCode() % numPartitions;
			}
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class SaveTweetsGroupingComparator extends
			WritableComparator {

		protected SaveTweetsGroupingComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			Text t1 = (Text) o1;
			Text t2 = (Text) o2;
			if (t1 == null || t2 == null)
				return 0;
			return t1.toString()
					.compareTo(t2.toString());
		}

	}

	public static class SaveTweetsMapper extends
			Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			int tabLocation = value.toString().indexOf('\t');
			String tid = (value.toString().substring(0, tabLocation));
			String tweetText = value.toString().substring(tabLocation + 1);
			tweetText = tweetText.replace("\"", "");
			tweetText = tweetText.replace("\'", "");
			tweetText = tweetText.replace("\n", "");
			tweetText = tweetText.replace("\t", "");
			context.write(new Text(tid), new Text(tweetText));
		}
	}

	public static class SaveTweetsReducer extends
			Reducer<Text, Text, Text, Text> {

 
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();

			if (!valuesIt.hasNext())
				return;

			while (valuesIt.hasNext()) {
				String value = valuesIt.next().toString();
				context.write(new Text(""), new Text("\"" + key.toString() + "\"" + " : \"" + value + "\"" + ","));
			}

		}

	}
}
