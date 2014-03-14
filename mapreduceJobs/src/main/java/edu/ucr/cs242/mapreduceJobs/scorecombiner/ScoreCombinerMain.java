package edu.ucr.cs242.mapreduceJobs.scorecombiner;

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


public class ScoreCombinerMain extends Configured implements Tool {

    public static final double eps = 0.001;
    public static final long counterReciprocal = 100;


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ScoreCombinerMain(), args);
        System.exit(res);
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

    	//inputfolder outputfolder lucenedir wordcountfile mincount
		if (args.length < 3)
			return 0;
		
		
		Job lJob = ScoreCombiner.createJob(new Path(args[0]), new Path(args[1]));
		Path outputPath = new Path(args[2]);
		FileSystem hdfs = FileSystem.get(lJob.getConfiguration());
		if (hdfs.exists(outputPath))
			hdfs.delete(outputPath, true);

		FileOutputFormat.setOutputPath(lJob, outputPath);
		boolean jobCompleted = lJob.waitForCompletion(true);
		if (!jobCompleted)
			return 0;
		
		
		return 1;
	}
}