package edu.ucr.cs242.mapreduceJobs.buildluceneindexes;

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

import edu.ucr.cs242.mapreduceJobs.getWordCounts.WordCounts;
import edu.ucr.cs242.mapreduceJobs.getWordCounts.WordCountsMain;

public class LuceneMain extends Configured implements Tool {

    public static final double eps = 0.001;
    public static final long counterReciprocal = 100;

    private static Logger log = Logger.getLogger(WordCountsMain.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordCountsMain(), args);
        System.exit(res);
    }


    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length < 4)
			return 0;

		log.info("Staring Lucene Job");
		Job wcJob = Lucene.createJob(args[2],args[3]);
		
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