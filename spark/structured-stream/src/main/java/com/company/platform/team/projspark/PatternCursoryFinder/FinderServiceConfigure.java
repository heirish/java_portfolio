package com.company.platform.team.projspark.PatternCursoryFinder;

public class FinderServiceConfigure {
    private String serviceType;
    public double leafSimilarity;

    //for kafka source
    private String kafkaBrokers;
    private String kafkaTopics;


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
}
