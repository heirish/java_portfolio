package com.company.platform.team.projspark.utils;

import com.company.platform.team.projspark.data.AppParameters;
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
        try {
            Configuration conf = new Configuration();
            if (!StringUtils.isEmpty(appParameters.inputfilter)) {
                conf.set("file.pattern", StringEscapeUtils.escapeJava(appParameters.inputfilter));
            }
            Job job = Job.getInstance(conf, "PatternRetrieve");
            job.setJarByClass(PatternRetriever.class);
            job.setMapperClass(PatternRetriever.ParentNodeMapper.class);
            job.setCombinerClass(PatternRetriever.PatternRetrieveReducer.class);
            job.setReducerClass(PatternRetriever.PatternRetrieveReducer.class);
            job.setOutputKeyClass(Text.class);
            //job.setOutputValueClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            Path input = new Path(appParameters.inputDir);
            System.out.println(input.toString());
            FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
            FileInputFormat.addInputPath(job, new Path(appParameters.inputDir));
            //job.setInputFormatClass(TextInputFormat.class);

            // FileOutputFormat.setOutputPath(job, new Path(appParameters.outputDir));
            // For test
            FileOutputFormat.setOutputPath(job,
                    new Path(appParameters.outputDir + "-" + System.currentTimeMillis()));
            job.waitForCompletion(true);
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
