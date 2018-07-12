package com.company.platform.team.projpatternreco.sparkandhadoop.patternerfiner;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class RefinerServiceConfigure {
    private static final Gson gson = new Gson();
    private long initialDelaySeconds;
    private long periodSeconds;

    private String fileSystemType;
    private String inputDir;
    private String inputfilter;
    private String outputDir;

    private int treeLevel;
    private double leafSimilarity;
    private double similarityDecayFactor;

    //for test
    private String visualTreePath;
    private String treePath;
    private String hadoopResource;
    private String projectFilter;

    public long getInitialDelaySeconds() {
        return initialDelaySeconds;
    }

    public long getPeriodSeconds() {
        return periodSeconds;
    }

    public String getFileSystemType() {
        return fileSystemType;
    }

    public String getInputDir() {
        return inputDir;
    }

    public String getInputfilter() {
        return inputfilter;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public int getTreeLevel() {
        return treeLevel;
    }

    public double getLeafSimilarity() {
        return leafSimilarity;
    }

    public double getSimilarityDecayFactor() {
        return similarityDecayFactor;
    }

    public static RefinerServiceConfigure parseFromJson(String jsonFile) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(jsonFile));
        RefinerServiceConfigure conf = gson.fromJson(br, RefinerServiceConfigure.class);
        return conf;
    }

    public String getVisualTreePath() {
        return visualTreePath;
    }

    public String getTreePath() {
        return treePath;
    }

    public String getHadoopResource() {
        return hadoopResource;
    }

    public String getProjectFilter() {
        return projectFilter;
    }
}
