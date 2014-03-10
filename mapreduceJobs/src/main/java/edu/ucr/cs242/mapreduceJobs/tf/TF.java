package edu.ucr.cs242.mapreduceJobs.tf;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.ucr.cs242.mapreduceJobs.pagerank.PageRank.PageRankComparator;
import edu.ucr.cs242.mapreduceJobs.pagerank.PageRank.PargeRankGroupingComparator;

public class TF {

	private static Logger log = Logger.getLogger(TF.class);

	public static Job createJob() throws IOException {
		Job job = new Job(new Configuration(), "WordCount");
		job.setJarByClass(TF.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(WordCountPartitioner.class);
		job.setMapperClass(WordCountMapper.class);
        job.setGroupingComparatorClass(WordCountGroupingComparator.class);
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountCombiner.class);
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
    public static class WordCountGroupingComparator extends WritableComparator {

        protected WordCountGroupingComparator() {
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

	public static class WordCountMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			int tabLocation = value.toString().indexOf('\t');
			String tid = (value.toString().substring(0, tabLocation));
			String tweetText = value.toString().substring(tabLocation + 1);

			String uid = key.toString();

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
				if (!set.contains(stemmedWord) && !stemmedWord.equals("")) {
					context.write(
							new Text(uid + ":" + tid + ":" + stemmedWord),
							new Text("1"));

				}

			}
		}
	}

	public static class WordCountCombiner extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();
			if (!valuesIt.hasNext())
				return;
			log.info("Preparing WordCounts");
			int count = 0;
			while (valuesIt.hasNext()) {
				String value = valuesIt.next().toString();
				count += Integer.parseInt(value);
			}

			if (true) {
				context.write(key, new Text(String.valueOf(count)));
			}
		}
	}

	public static class WordCountReducer extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();
			if (!valuesIt.hasNext())
				return;

			int count = 0;
			while (valuesIt.hasNext()) {
				String value = valuesIt.next().toString();
				count += Integer.parseInt(value);
			}
			String[] pieces = key.toString().split("\\:");

			context.write(new Text(pieces[2]), new Text(pieces[0] + ":"
					+ pieces[1] + ":" + count));
		}
	}
}
