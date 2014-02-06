package edu.ucr.cs242.mapreduceJobs.sampler;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SamplerMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SamplerMain(), args);
        System.exit(res);
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job samplerJob = RandomSampler.createJob();
        if (args.length < 5)
            return 0;
        
        //Sampling users
        samplerJob.getConfiguration().setLong("totalSize", Integer.valueOf(args[3]));
        samplerJob.getConfiguration().setLong("sampleSize", Integer.valueOf(args[4]));
        FileSystem hdfs = FileSystem.get(samplerJob.getConfiguration());
        Path outputPath1 = new Path(args[2]);
        if (hdfs.exists(outputPath1))
            hdfs.delete(outputPath1, true);
        FileInputFormat.addInputPath(samplerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(samplerJob, outputPath1);
        boolean jobCompleted = samplerJob.waitForCompletion(true);
        if (!jobCompleted)
            return 0;

        //Getting number of followers per user
        Job analyticsJob = Analytics.createFollowerCounterJob();
        Path outputPath2 = new Path(args[2] + "/analytics");
        if (hdfs.exists(outputPath2))
            hdfs.delete(outputPath2, true);
        FileInputFormat.addInputPath(analyticsJob, outputPath1);
        FileOutputFormat.setOutputPath(analyticsJob, outputPath2);
        jobCompleted = analyticsJob.waitForCompletion(true);
        if (!jobCompleted)
            return 0;

        //Getting histogramm distribution for the number of followers
        Job histogrammJob = Analytics.createFollowerHistorgammJob();
        Path outputPath3 = new Path(args[2] + "/histogramm");
        if (hdfs.exists(outputPath3))
            hdfs.delete(outputPath3, true);
        FileInputFormat.addInputPath(histogrammJob, outputPath2);
        FileOutputFormat.setOutputPath(histogrammJob, outputPath3);
        jobCompleted = histogrammJob.waitForCompletion(true);
        if (!jobCompleted)
            return 0;

        //Filtering user's screen names
        Job screenNameJoinJob = ScreenNameFilter.createJoinJob();
        Path outputPath4 = new Path(args[2] + "/screenNames");
        if (hdfs.exists(outputPath4))
            hdfs.delete(outputPath4, true);
        FileInputFormat.addInputPath(screenNameJoinJob, new Path(args[1]));
        FileInputFormat.addInputPath(screenNameJoinJob, outputPath2);
        FileOutputFormat.setOutputPath(screenNameJoinJob, outputPath4);
        jobCompleted = screenNameJoinJob.waitForCompletion(true);
        if (!jobCompleted)
            return 0;
        
        return 1;
    }
}