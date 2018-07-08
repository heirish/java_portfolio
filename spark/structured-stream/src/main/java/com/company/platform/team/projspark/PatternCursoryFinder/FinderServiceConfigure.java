package com.company.platform.team.projspark.PatternCursoryFinder;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileReader;

public class FinderServiceConfigure {
    private static final Gson gson = new Gson();
    private String serviceType;
    private double leafSimilarity;
    //for test
    private String projectFilter;

    //for kafka source
    private String kafkaBrokers;
    private String kafkaTopics;

    //outputType
    static class LogOut{
        public String type;
        public String path;
    }
    private LogOut logOut;


    //outputPatternType
    static class LogPatternOut {
        public String type;
        public String path;
    }
    private LogPatternOut logPatternOut;

    public String getProjectFilter() {
        return projectFilter;
    }

    public String getServiceType() {
        return serviceType;
    }

    public double getLeafSimilarity() {
        return leafSimilarity;
    }

    public String getKafkaBrokers() {
        return kafkaBrokers;
    }

    public String getKafkaTopics() {
        return kafkaTopics;
    }

    public String getLogOutType() {
        return logOut.type;
    }

    public String getLogOutPath() {
        return logOut.path;
    }

    public String getLogPatternOutType() {
        return logPatternOut.type;
    }

    public String getLogPatternOutPath() {
        return logPatternOut.path;
    }

    public static FinderServiceConfigure parseFromJson(String jsonFile)
            throws Exception{
        BufferedReader br = new BufferedReader(new FileReader(jsonFile));
        FinderServiceConfigure conf = gson.fromJson(br, FinderServiceConfigure.class);
        return conf;
    }
}
