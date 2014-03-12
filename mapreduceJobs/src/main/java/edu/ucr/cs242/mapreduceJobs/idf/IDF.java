package edu.ucr.cs242.mapreduceJobs.idf;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.ucr.cs242.mapreduceJobs.tf.Stemmer;

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
		job.setCombinerClass(IDFCombiner.class);

		return job;
	}

	public static final class IDFPartitioner extends Partitioner<Text, Text> {

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

			int tabLocation = value.toString().indexOf('\t');
			String tweetText = value.toString().substring(tabLocation + 1);

			Set<String> foundWords = new HashSet<String>();

			for (String virginWord : tweetText.split("\\s+")) {
				// Remove all special characters
				String word = virginWord.replaceAll("[^a-zA-Z0-9]", "");

				// Make lower case
				word = word.toLowerCase();

				// stem
				char[] w = new char[word.length()];
				for (int i = 0; i < word.length(); i++) {
					w[i] = word.charAt(i);
				}
				Stemmer s = new Stemmer();
				s.add(w, w.length);
				s.stem();
				String stemmedWord = s.toString();

				// Check if stop word
				Set<Object> set = StandardAnalyzer.STOP_WORDS_SET;
				if (!set.contains(stemmedWord) && !stemmedWord.equals("")
						&& !foundWords.contains(stemmedWord)) {
					foundWords.add(stemmedWord);
					context.write(new Text(stemmedWord), new Text("1"));
				}

			}
		}
	}

	public static class IDFCombiner extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();

			double df = 0;

			if (!valuesIt.hasNext())
				return;

			while (valuesIt.hasNext()) {
				df += Double.parseDouble(valuesIt.next().toString());
			}

			context.write(key, new Text(String.valueOf(df)));
		}
	}

	public static class IDFReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();

			double N = 10326522;
			double df = 0;

			if (!valuesIt.hasNext())
				return;

			while (valuesIt.hasNext()) {
				df += Double.parseDouble(valuesIt.next().toString());
			}

			double idf = Math.log(N / df);
			context.write(key, new Text(String.valueOf(idf)));
		}
	}
}
