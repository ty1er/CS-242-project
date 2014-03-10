package edu.ucr.cs242.mapreduceJobs.tf;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFMain extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TFMain(), args);
		System.exit(res);
	}

	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {

		// inputfolder outputfolder sortoutputfolder minCount
		if (args.length < 2)
			return 0;

		Job wcJob = TF.createJob();
		Path outputPath = new Path(args[1]);
		Path inputPath = new Path(args[0]);
		FileSystem hdfs = FileSystem.get(wcJob.getConfiguration());
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		FileInputFormat.addInputPath(wcJob, inputPath);
		FileOutputFormat.setOutputPath(wcJob, outputPath);
		boolean jobCompleted = wcJob.waitForCompletion(true);
		if (!jobCompleted)
			return 0;

		return 1;
	}
}