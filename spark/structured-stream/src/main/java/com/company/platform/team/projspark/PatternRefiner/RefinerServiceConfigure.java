package com.company.platform.team.projspark.PatternRefiner;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class RefinerServiceConfigure {
    private long initialDelaySeconds;
    private long periodSeconds;

    private String inputDir;
    private String inputfilter;
    private String outputDir;

    private int treeLevel;
    double leafSimilarity;
    double similarityDecayFactor;

    public long getInitialDelaySeconds() {
        return initialDelaySeconds;
    }

    public long getPeriodSeconds() {
        return periodSeconds;
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
}
