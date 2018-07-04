package com.company.platform.team.projspark.utils;

import com.company.platform.team.projspark.data.AppParameters;
import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.data.PatternLevelTree;
import com.company.platform.team.projspark.modules.PatternRetriever;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2018/7/1 0001.
 */
public class PatternRetrieveTask implements Runnable {
    private AppParameters appParameters;

    public PatternRetrieveTask(AppParameters parameters) throws Exception{
        if (parameters == null) {
            throw new Exception("invalid parameters");
        }
        this.appParameters = parameters;
    }

    @Override
    public void run() {
        //for Test
        String lastOutput = appParameters.outputDir + "-0";
        //or by project, by level
        for (int i = 0; i < Constants.MAX_PATTERN_LEVEL; i++) {
        //for (int i = 0; i < 2; i++) {
            try {
                Configuration conf = new Configuration();
                if (!StringUtils.isEmpty(appParameters.inputfilter)) {
                    conf.set("file.pattern", StringEscapeUtils.escapeJava(appParameters.inputfilter));
                }
                conf.set("level", String.valueOf(i));
                double maxDist = 1-appParameters.leafSimilarity*(Math.pow(appParameters.similarityDecayFactor, i));
                conf.set("level.maxDist", String.valueOf(maxDist));
                System.out.println(String.format("level :%s, maxDists: %s", i, maxDist));

                Job job = Job.getInstance(conf, "PatternRetrieve");
                job.setJarByClass(PatternRetriever.class);
                job.setMapperClass(PatternRetriever.ParentNodeMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                //job.setCombinerClass(PatternRetriever.PatternRetrieveReducer.class);
                job.setReducerClass(PatternRetriever.PatternRetrieveReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);

                //TODO: if (i == 0) { set inputDir } else read Nodes from tree
                FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
                if (i == 0) {
                    FileInputFormat.addInputPath(job, new Path(appParameters.inputDir));
                } else {
                    FileInputFormat.addInputPath(job,
                            new Path(appParameters.outputDir + "-" + String.valueOf(i-1)));
                }

                // For test
                //first delete if outputdir exists
                String outputPath = appParameters.outputDir + "-" + String.valueOf(i);
                try {
                    File file = new File(outputPath);
                    FileUtils.deleteDirectory(file);
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
                FileOutputFormat.setOutputPath(job, new Path(outputPath));

                job.waitForCompletion(true);
                //TODO: if (i==0) { remove inputDir}
                PatternLevelTree.getInstance().saveTreeToFile("./patterntree");
                System.out.println("Sleeping ...");
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } catch (ClassNotFoundException cnfe) {
                cnfe.printStackTrace();
            } catch (InterruptedException inte) {
                inte.printStackTrace();
            }
        }
    }
}
