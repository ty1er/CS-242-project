package edu.ucr.cs242.mapreduceJobs.scorecombiner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.ucr.cs242.mapreduceJobs.turank.TURankPreparation.TURankRetweetMapper;
import edu.ucr.cs242.mapreduceJobs.turank.TURankPreparation.TURankTweetMapper;

public class ScoreCombiner {

	public static Job createJob(Path tfidfpath, Path turankpath) throws IOException {
		Job job = new Job(new Configuration(), "ScoreCombiner");
		job.setJarByClass(ScoreCombiner.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setSortComparatorClass(ScoreCombinerComparator.class);
		job.setGroupingComparatorClass(ScoreCombinerGroupingComparator.class);
		job.setPartitionerClass(ScoreCombinerPartitioner.class);
        MultipleInputs.addInputPath(job, turankpath, KeyValueTextInputFormat.class, TURANKMapper.class);
        MultipleInputs.addInputPath(job, tfidfpath, KeyValueTextInputFormat.class, TFIDFMapper.class);
		job.setReducerClass(ScoreCombinerReducer.class);

		return job;
	}

	public static class ScoreCombinerComparator extends WritableComparator {

		protected ScoreCombinerComparator() {
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
				return t1.toString().substring(t1.find(":") + 1)
						.compareTo(t2.toString().substring(t2.find(":") + 1));
			}
			return comp;
		}

	}

	public static final class ScoreCombinerPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString().substring(0, key.find(":"));
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class ScoreCombinerGroupingComparator extends
			WritableComparator {

		protected ScoreCombinerGroupingComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			Text t1 = (Text) o1;
			Text t2 = (Text) o2;
			if (t1 == null || t2 == null)
				return 0;
			return t1.toString().substring(0, t1.find(":"))
					.compareTo(t2.toString().substring(0, t2.find(":")));
		}

	}

	public static class TFIDFMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String word = key.toString();
			String[] pieces = value.toString().split("\\:");
			context.write(new Text(pieces[0] + ":1"), new Text(word + ":"
					+ pieces[1]));

		}
	}

	public static class TURANKMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String tid = key.toString();
			double score = Double.parseDouble(value.toString());
			if (score <= 0.18503099884985902) {
				score = 0;
			} else if (score >= 81.62800157711727) {
				score = 1;
			} else {
				score = ((score - 0.18503099884985902) / (81.62800157711727 - 0.18503099884985902));
			}
			context.write(new Text(tid + ":0"), new Text(String.valueOf(score)));
		}
	}

	public static class ScoreCombinerReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();

			if (!valuesIt.hasNext())
				return;

			String test = valuesIt.next().toString();

			double tuRank = Double.parseDouble(test);
			String tid = key.toString().substring(0, key.find(":"));

			while (valuesIt.hasNext()) {
				String value = valuesIt.next().toString();
				String[] pieces = value.split("\\:");
				String word = pieces[0];
				double tfidf = Double.parseDouble(pieces[1]);
				double score = tuRank + tfidf;
				context.write(new Text(word), new Text(tid + ":" + score));
			}

		}
	}
}
