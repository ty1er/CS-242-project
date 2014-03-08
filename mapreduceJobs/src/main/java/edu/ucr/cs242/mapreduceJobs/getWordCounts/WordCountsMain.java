package edu.ucr.cs242.mapreduceJobs.getWordCounts;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WordCountsMain extends Configured implements Tool {

    public static final double eps = 0.001;
    public static final long counterReciprocal = 100;


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordCountsMain(), args);
        System.exit(res);
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

    	//inputfolder outputfolder sortoutputfolder minCount
		if (args.length < 2)
			return 0;

		Job wcJob = WordCounts.createJob();
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
		
		
		
		Job sJob = Sorter.createJob();
		outputPath = new Path(args[2]);
		inputPath = new Path(args[1]);
		hdfs = FileSystem.get(sJob.getConfiguration());
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);
		
		FileInputFormat.addInputPath(sJob, inputPath);
		FileOutputFormat.setOutputPath(sJob, outputPath);
		jobCompleted = sJob.waitForCompletion(true);
		if (!jobCompleted)
			return 0;
		
		
		return 1;
	}
}