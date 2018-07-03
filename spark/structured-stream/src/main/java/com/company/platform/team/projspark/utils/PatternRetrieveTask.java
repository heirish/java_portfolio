package com.company.platform.team.projspark.utils;

import com.company.platform.team.projspark.data.AppParameters;
import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.data.PatternLevelTree;
import com.company.platform.team.projspark.modules.PatternRetriever;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
        //or by project, by level
        for (int i = 0; i < Constants.MAX_PATTERN_LEVEL; i++) {
            try {
                Configuration conf = new Configuration();
                if (!StringUtils.isEmpty(appParameters.inputfilter)) {
                    conf.set("file.pattern", StringEscapeUtils.escapeJava(appParameters.inputfilter));
                }
                conf.set("level", String.valueOf(i));
                conf.set("level.maxDist",
                        String.valueOf(1-appParameters.leafSimilarity*(Math.pow(appParameters.similarityDecayFactor, i))));

                Job job = Job.getInstance(conf, "PatternRetrieve");
                job.setJarByClass(PatternRetriever.class);
                job.setMapperClass(PatternRetriever.ParentNodeMapper.class);
                job.setCombinerClass(PatternRetriever.PatternRetrieveReducer.class);
                job.setReducerClass(PatternRetriever.PatternRetrieveReducer.class);
                job.setOutputKeyClass(Text.class);
                //job.setOutputValueClass(IntWritable.class);
                job.setOutputValueClass(Text.class);

                //TODO: if (i == 0) { set inputDir } else read Nodes from tree
                Path input = new Path(appParameters.inputDir);
                System.out.println(input.toString());
                FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
                FileInputFormat.addInputPath(job, new Path(appParameters.inputDir));

                // For test
                FileOutputFormat.setOutputPath(job,
                        new Path(appParameters.outputDir + "-" + System.currentTimeMillis()));
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
