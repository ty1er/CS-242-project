package edu.ucr.cs242.mapreduceJobs.hits.orig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class HitsMain extends Configured implements Tool {

    public static final double eps = 0.001;
    public static final long counterReciprocal = 100;

    private static Logger log = Logger.getLogger(HitsMain.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new HitsMain(), args);
        System.exit(res);
    }

    public static enum IterationCounter {
        RESIDUAL
    };

    public int run(String[] args) throws Exception {

        if (args.length < 2)
            return 0;

        Job preparationJob = HitsOrigPreparation.createJob();

        //preparing pageRank first iteration
        FileSystem hdfs = FileSystem.get(preparationJob.getConfiguration());
        Path outputPath1 = new Path(args[1] + "/iteration0");
        if (hdfs.exists(outputPath1))
            hdfs.delete(outputPath1, true);
        FileInputFormat.addInputPath(preparationJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preparationJob, outputPath1);
        log.info("Preparing Hits interations");
        boolean jobCompleted = preparationJob.waitForCompletion(true);
        if (!jobCompleted)
            return 0;

        boolean converged = false;
        int iterationCounter = 1;
        while (!converged) {
            log.info("Staring Hits interation #" + iterationCounter);
            //Getting number of followers per user
            Job hitsIterationJob = HitsOrig.createJob();

            Path outputPath2 = new Path(args[1] + "/iteration" + iterationCounter);
            if (hdfs.exists(outputPath2))
                hdfs.delete(outputPath2, true);
            FileInputFormat.addInputPath(hitsIterationJob, outputPath1);
            FileOutputFormat.setOutputPath(hitsIterationJob, outputPath2);
            jobCompleted = hitsIterationJob.waitForCompletion(true);
            if (!jobCompleted)
                return 0;

            Job hitsNormCalcJob = HitsNormCalc.createJob();

            Path outputPath3 = new Path(args[1] + "/iterationNormCalc" + iterationCounter);
            if (hdfs.exists(outputPath3))
                hdfs.delete(outputPath3, true);
            FileInputFormat.addInputPath(hitsNormCalcJob, outputPath2);
            FileOutputFormat.setOutputPath(hitsNormCalcJob, outputPath3);
            jobCompleted = hitsNormCalcJob.waitForCompletion(true);
            if (!jobCompleted)
                return 0;
            List<Double> norms = readResults(hitsNormCalcJob.getConfiguration(),
                    new Path(outputPath3 + "/part-r-00000"));
            double authNorm = norms.get(0);
            double hubNorm = norms.get(1);
            hdfs.delete(outputPath3, true);

            Job hitsNormalizeJob = HitsNormalization.createJob();
            hitsNormalizeJob.getConfiguration().setDouble("authNorm", authNorm);
            hitsNormalizeJob.getConfiguration().setDouble("hubNorm", hubNorm);

            Path outputPath4 = new Path(args[1] + "/iterationNorm" + iterationCounter);
            if (hdfs.exists(outputPath4))
                hdfs.delete(outputPath4, true);
            FileInputFormat.addInputPath(hitsNormalizeJob, outputPath1);
            FileInputFormat.addInputPath(hitsNormalizeJob, outputPath2);
            FileOutputFormat.setOutputPath(hitsNormalizeJob, outputPath4);
            jobCompleted = hitsNormalizeJob.waitForCompletion(true);
            if (!jobCompleted)
                return 0;

            hdfs.delete(outputPath1, true);
            hdfs.delete(outputPath2, true);
            outputPath1 = outputPath4;
            converged = hitsNormalizeJob.getCounters().findCounter(IterationCounter.RESIDUAL).getValue() < 1;
            iterationCounter++;
        }

        return 1;
    }

    private List<Double> readResults(Configuration jconf, Path pathIn) throws Exception {
        List<Double> result = new ArrayList<Double>();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(jconf, Reader.file(pathIn));
            Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), jconf);
            DoubleWritable value = (DoubleWritable) ReflectionUtils.newInstance(reader.getValueClass(), jconf);

            while (reader.next(key, value)) {
                // System.out.printf("%s\t%s\n", key, value);
                if (key.toString().equals("auth")) {
                    result.add(0, value.get());
                } else if (key.toString().equals("hub")) {
                    result.add(1, value.get());
                } else {
                    throw new Exception("Unexpected value of normalization term");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }

        return result;
    }
}
