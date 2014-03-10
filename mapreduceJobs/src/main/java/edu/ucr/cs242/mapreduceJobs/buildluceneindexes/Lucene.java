package edu.ucr.cs242.mapreduceJobs.buildluceneindexes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.index.IndexWriter;

public class Lucene {

	public static Job createJob() throws IOException {
		Job job = new Job(new Configuration(), "WordCounts");
		job.setJarByClass(Lucene.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(WordCountPartitioner.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// initialize wordmap

		return job;
	}

	public static final class WordCountPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString();
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class WordCountMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] pieces = value.toString().split("\\:");
			String uid = pieces[0];
			String tid = pieces[1];
			String count = pieces[2];
			String word = key.toString();

			context.write(new Text(word), new Text(uid + ":" + tid + ":"
					+ count));

		}
	}

	public static class WordCountReducer extends
			Reducer<Text, Text, Text, Text> {

		private IndexWriter writer;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();

			double N = 10326522;
			double df = 0;
			Map<String, String> tfs = new HashMap<String, String>();

			if (!valuesIt.hasNext())
				return;

			while (valuesIt.hasNext()) {
				String value = valuesIt.next().toString();
				String[] pieces = value.split("\\:");
				df++;
				tfs.put((pieces[0] + ":" + pieces[1]), pieces[2]);
			}

			for (String document : tfs.keySet()) {
				double tf = Double.parseDouble(tfs.get(document));
				double idf = Math.log(N / df);
				double tfidf = tf * idf;
				context.write(key, new Text(document + ":" + tfidf));

			}
		}
	}
}
