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
        Path outputPath = new Path(args[1]);
        if (hdfs.exists(outputPath))
            hdfs.delete(outputPath, true);
        FileInputFormat.addInputPath(samplerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(samplerJob, outputPath);
        // To set the number of mapper in the Fagin Algorithm, just cancal the commit symbol
        // sortingJob.setNumReduceTasks(2);
        boolean sortingJobCompletion = samplerJob.waitForCompletion(true);
        if (!sortingJobCompletion)
            return 0;

        return 1;
    }
}