package edu.ucr.cs242.mapreduceJobs.pagerank;

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

public class PageRankMain extends Configured implements Tool {

    public static final double eps = 0.001;
    public static final long counterReciprocal = 100;

    private static Logger log = Logger.getLogger(PageRankMain.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PageRankMain(), args);
        System.exit(res);
    }

    public static enum IterationCounter {
        RESIDUAL
    };

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 2)
            return 0;

        Job preparationJob = PageRankPreparation.createJob();

        //preparing pageRank first iteration
        FileSystem hdfs = FileSystem.get(preparationJob.getConfiguration());
        Path outputPath1 = new Path(args[1] + "/iteration0");
        if (hdfs.exists(outputPath1))
            hdfs.delete(outputPath1, true);
        FileInputFormat.addInputPath(preparationJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preparationJob, outputPath1);
        log.info("Preparing PageRank interations");
        boolean jobCompleted = preparationJob.waitForCompletion(true);
        if (!jobCompleted)
            return 0;

        boolean converged = false;
        int iterationCounter = 1;
        while (!converged) {
            log.info("Staring PageRank interation #" + iterationCounter);
            //Getting number of followers per user
            Job prIterationJob = PageRank.createJob();
            //            prIterationJob.getCounters().findCounter(IterationCounter.RESIDUAL).setValue(0);

            Path outputPath2 = new Path(args[1] + "/iteration" + iterationCounter);
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

        return 1;
    }
}