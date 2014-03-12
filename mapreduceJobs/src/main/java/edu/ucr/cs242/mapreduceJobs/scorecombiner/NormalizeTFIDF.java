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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NormalizeTFIDF {

	public static Job createJob() throws IOException {
		Job job = new Job(new Configuration(), "NormalizeTFIDF");
		job.setJarByClass(NormalizeTFIDF.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setGroupingComparatorClass(NormalizeTFIDFGroupingComparator.class);
		job.setPartitionerClass(NormalizeTFIDFPartitioner.class);
		job.setMapperClass(NormalizeTFIDFMapper.class);
		job.setReducerClass(NormalizeTFIDFReducer.class);

		return job;
	}

	public static final class NormalizeTFIDFPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String newKey = key.toString();
			return newKey.hashCode() % numPartitions;
		}
	}

	public static class NormalizeTFIDFGroupingComparator extends
			WritableComparator {

		protected NormalizeTFIDFGroupingComparator() {
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

	public static class NormalizeTFIDFMapper extends
			Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] pieces = value.toString().split("\\:");
			double tfidf = Double.parseDouble(pieces[1]);
			if (tfidf >= 494.130787450104){
				tfidf = 1;
			}
			else if (tfidf <= 2.011063703278 ){
				tfidf = 0;
			}
			else{
				tfidf = ((tfidf - 2.011063703278 ) / (494.130787450104 - 2.011063703278));
			}
				context.write(key, new Text(pieces[0] + ":" + tfidf));
		}
	}

	public static class NormalizeTFIDFReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();

			if (!valuesIt.hasNext())
				return;

			while (valuesIt.hasNext()) {
				String value = valuesIt.next().toString();
				context.write(key, new Text(value));
			}

		}
	}
}
