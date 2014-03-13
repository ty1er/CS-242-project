package edu.ucr.cs242.mapreduceJobs.turank;

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

public class TURankMain extends Configured implements Tool {

    public static final double eps = 0.001;
    public static final long counterReciprocal = 100;

    private static Logger log = Logger.getLogger(TURankMain.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TURankMain(), args);
        System.exit(res);
    }

    public static enum IterationCounter {
        RESIDUAL
    };

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 4)
            return 0;

        Job preparationJob = TURankPreparation.createJob(new Path(args[0]), new Path(args[1]), new Path(args[2]));

        //preparing TURank first iteration
        FileSystem hdfs = FileSystem.get(preparationJob.getConfiguration());
        Path outputPath1 = new Path(args[3] + "/iteration33");
        if (hdfs.exists(outputPath1))
            hdfs.delete(outputPath1, true);
        FileOutputFormat.setOutputPath(preparationJob, outputPath1);
        log.info("Preparing TURank interations");
        boolean jobCompleted = preparationJob.waitForCompletion(true);
        if (!jobCompleted)
            return 0;

        boolean converged = false;
        int iterationCounter = 1;
        while (!converged) {
            log.info("Staring TURank interation #" + iterationCounter);
            //Getting number of followers per user
            Job prIterationJob = TURank.createJob();

            Path outputPath2 = new Path(args[3] + "/iteration" + iterationCounter);
            if (hdfs.exists(outputPath2))
                hdfs.delete(outputPath2, true);
            FileInputFormat.addInputPath(prIterationJob, outputPath1);
            FileOutputFormat.setOutputPath(prIterationJob, outputPath2);
            jobCompleted = prIterationJob.waitForCompletion(true);
            if (!jobCompleted)
                return 0;
            converged = prIterationJob.getCounters().findCounter(IterationCounter.RESIDUAL).getValue() < 1;
            iterationCounter++;
            hdfs.delete(outputPath1, true);
            outputPath1 = outputPath2;
        }

        Job filteringJob = TuRankFilter.createJob();
        Path outputPath3 = new Path(args[3] + "/result");
        if (hdfs.exists(outputPath3))
            hdfs.delete(outputPath3, true);
        log.info("Final TURank filtering");
        FileInputFormat.addInputPath(filteringJob, outputPath1);
        FileOutputFormat.setOutputPath(filteringJob, outputPath3);
        jobCompleted = filteringJob.waitForCompletion(true);
        if (!jobCompleted)
            return 0;

        return 1;
    }
}