package com.company.platform.team.projspark.PatternRefiner;

import com.company.platform.team.projspark.common.data.Constants;
import com.company.platform.team.projspark.common.utils.CommonUtils;
import com.company.platform.team.projspark.common.utils.HdfsUtil;
import com.company.platform.team.projspark.common.utils.RegexPathFilter;
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
 * Created by Administrator on 2018/7/7 0007.
 */
public class MapReduceRefiner implements Runnable {
    private RefinerServiceConfigure refinderConf;
    private String jobName;

    public MapReduceRefiner(RefinerServiceConfigure conf, String name) {
        this.refinderConf= conf;
        this.jobName = name;
    }

    @Override
    public void run() {
        //or by project, by level
        for (int i = 0; i < Constants.MAX_PATTERN_LEVEL; i++) {
            //for (int i = 0; i < 2; i++) {
            try {
                Configuration conf = new Configuration();
                conf.set("file.keepdir", "true");
                if (!StringUtils.isEmpty(refinderConf.getHadoopResource())) {
                    conf.addResource(new Path(refinderConf.getHadoopResource()));
                }
                if (!StringUtils.isEmpty(refinderConf.getInputfilter())) {
                    conf.set("file.pattern",
                            StringEscapeUtils.escapeJava(refinderConf.getInputfilter()));
                }
                conf.set("level", String.valueOf(i));
                double maxDist = 1-refinderConf.getLeafSimilarity()
                        *(Math.pow(refinderConf.getSimilarityDecayFactor(), i));
                conf.set("level.maxDist", String.valueOf(maxDist));
                System.out.println(String.format("level :%s, maxDists: %s", i, maxDist));

                Job job = Job.getInstance(conf, "PatternRetrieve");
                job.setJarByClass(MapReduceRefinerWorker.class);
                job.setMapperClass(MapReduceRefinerWorker.ParentNodeMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                //job.setCombinerClass(PatternRetriever.PatternRetrieveReducer.class);
                job.setReducerClass(MapReduceRefinerWorker.PatternRetrieveReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);

                //if (i == 0) { set inputDir } else read Nodes from tree
                FileInputFormat.setInputPathFilter(job, RegexPathFilter.class);
                String inputPath = refinderConf.getOutputDir()+ "-" + String.valueOf(i);
                if (i == 0) {
                    //There is no new coming files
                    int movedFileCount = 0;
                    if (StringUtils.equalsIgnoreCase(refinderConf.getFileSystemType(), "hdfs")) {
                        System.out.println("input Path: " + refinderConf.getInputDir());
                        movedFileCount = HdfsUtil.moveFilesAlreadyCompleted(refinderConf.getInputDir(),
                                inputPath, "(.*.json)|(.*.crc)",
                                refinderConf.getHadoopResource());
                    } else {
                        movedFileCount = CommonUtils.moveFilesAlreadyCompleted(refinderConf.getInputDir(),
                                inputPath, "(.*.json)|(.*.crc)");
                    }
                    if (movedFileCount <= 0) {
                        break;
                    }
                }
                FileInputFormat.addInputPath(job, new Path(inputPath));

                // For test
                //first delete if outputdir exists
                String outputPath = refinderConf.getOutputDir()+ "-" + String.valueOf(i+1);
                try {
                    if (StringUtils.equalsIgnoreCase(refinderConf.getFileSystemType(), "hdfs")) {
                        HdfsUtil.deleteDir(outputPath);
                    } else {
                        File file = new File(outputPath);
                        FileUtils.deleteDirectory(file);
                    }
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
                FileOutputFormat.setOutputPath(job, new Path(outputPath));

                job.waitForCompletion(true);
                //for test
                if (i==0) {
                    if (StringUtils.equalsIgnoreCase(refinderConf.getFileSystemType(), "hdfs")) {
                        HdfsUtil.deleteDir(inputPath);
                    } else {
                        FileUtils.deleteDirectory(new File(inputPath));
                    }
                }
                //TODO:synchronize pattern tree to database
                System.out.println("Sleeping ...");
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } catch (ClassNotFoundException cnfe) {
                cnfe.printStackTrace();
            } catch (InterruptedException inte) {
                inte.printStackTrace();
            }
        }
        PatternLevelTree.getInstance().saveTreeToFile(refinderConf.getVisualTreePath(), refinderConf.getProjectFilter());
        PatternLevelTree.getInstance().backupTree(refinderConf.getTreePath());
    }
}
