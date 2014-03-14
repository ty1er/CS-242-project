package edu.ucr.cs242.mapreduceJobs.hadoopindex;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class HadoopIndex {

	public static Job createJob() throws IOException {
		Job job = new Job(new Configuration(), "HadoopIndex");
		job.setJarByClass(HadoopIndex.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setGroupingComparatorClass(HadoopIndexGroupingComparator.class);
		job.setPartitionerClass(HadoopIndexPartitioner.class);
		job.setMapperClass(HadoopIndexMapper.class);
		job.setReducerClass(HadoopIndexReducer.class);
		job.setNumReduceTasks(1000);

		return job;
	}

	public static final class HadoopIndexPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString();
			if (newKey.length() > 5){
			return newKey.substring(newKey.length()-6, newKey.length()-1).hashCode() % numPartitions;
			}
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class HadoopIndexGroupingComparator extends WritableComparator {

		protected HadoopIndexGroupingComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			Text t1 = (Text) o1;
			Text t2 = (Text) o2;
			String a = (t1.toString());
			String b = (t2.toString());
			if (t1 == null || t2 == null)
				return 0;
			return a.compareTo(b);
			
		}

	}

	public static class HadoopIndexMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}

	}

	public static class HadoopIndexReducer extends Reducer<Text, Text, Text, Text> {
		

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();

			if (!valuesIt.hasNext())
				return;
			
			String word = key.toString();
			String tidColonScores = valuesIt.next().toString();
			
			while (valuesIt.hasNext()) {
				String tidColonScore = "," + valuesIt.next().toString();
				tidColonScores += tidColonScore;
				context.progress();
			}
			context.write(new Text(""), new Text("\"" + word + "\"" + " : \"" + tidColonScores + "\"" + ","));

		}
		
	}
}
