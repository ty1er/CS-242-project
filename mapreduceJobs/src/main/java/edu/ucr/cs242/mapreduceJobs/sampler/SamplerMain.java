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
        if (args.length < 4)
            return 0;
        
        samplerJob.getConfiguration().setLong("totalSize", Integer.valueOf(args[2]));
        samplerJob.getConfiguration().setLong("sampleSize", Integer.valueOf(args[3]));
        FileSystem hdfs = FileSystem.get(samplerJob.getConfiguration());
        Path outputPath1 = new Path(args[1]);
        if (hdfs.exists(outputPath1))
            hdfs.delete(outputPath1, true);
        FileInputFormat.addInputPath(samplerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(samplerJob, outputPath1);
        boolean sortingJobCompletion = samplerJob.waitForCompletion(true);
        if (!sortingJobCompletion)
            return 0;

        Job analyticsJob = Analytics.createCounterJob();
        Path outputPath2 = new Path(args[1] + "/analytics");
        if (hdfs.exists(outputPath2))
            hdfs.delete(outputPath2, true);
        FileInputFormat.addInputPath(analyticsJob, outputPath1);
        FileOutputFormat.setOutputPath(analyticsJob, outputPath2);
        sortingJobCompletion = analyticsJob.waitForCompletion(true);
        if (!sortingJobCompletion)
            return 0;

        Job histogrammJob = Analytics.createHistorgammJob();
        Path outputPath3 = new Path(args[1] + "/histogramm");
        if (hdfs.exists(outputPath3))
            hdfs.delete(outputPath3, true);
        FileInputFormat.addInputPath(histogrammJob, outputPath2);
        FileOutputFormat.setOutputPath(histogrammJob, outputPath3);
        sortingJobCompletion = histogrammJob.waitForCompletion(true);
        if (!sortingJobCompletion)
            return 0;

        return 1;
    }
}