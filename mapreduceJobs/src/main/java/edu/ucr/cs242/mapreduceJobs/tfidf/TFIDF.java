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

		job.setSortComparatorClass(TFIDFComparator.class);
		job.setGroupingComparatorClass(TFIDFGroupingComparator.class);
		job.setPartitionerClass(TFIDFPartitioner.class);
		job.setMapperClass(TFIDFMapper.class);
		job.setReducerClass(TFIDFReducer.class);

		return job;
	}

	public static class TFIDFComparator extends WritableComparator {

		protected TFIDFComparator() {
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

	public static final class TFIDFPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString().substring(0, key.find(":"));
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class TFIDFGroupingComparator extends WritableComparator {

		protected TFIDFGroupingComparator() {
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

			// check if it is a TF or an TFIDF, artificially change key
			if (value.toString().contains(":")) {
				context.write(new Text(key.toString() + ":1"), value);
			} else {
				context.write(new Text(key.toString() + ":0"), value);
			}

		}
	}

	public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();

			if (!valuesIt.hasNext())
				return;

			String test = valuesIt.next().toString();
			//if (test.contains(":")) {
			//} else {
				double idf = Double.parseDouble(test);
				String keyword = key.toString().substring(0, key.find(":"));

				while (valuesIt.hasNext()) {
					String value = valuesIt.next().toString();
					String[] pieces = value.split("\\:");
					String tid = pieces[1];
					double tf = Double.parseDouble(pieces[2]);
					double tfidf = tf * idf;
					context.write(new Text(keyword),
							new Text(tid + ":" + tfidf));
				}
			//}

		}
	}
}
