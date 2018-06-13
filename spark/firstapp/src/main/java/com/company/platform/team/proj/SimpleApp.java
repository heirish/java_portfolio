package com.company.platform.team.proj;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
/**
 * Created by admin on 2018/6/13.
 */
public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "/home/heirish/spark/README.md";
        SparkSession session = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = session.read().textFile(logFile).cache();

        long numAs = logData.filter(s->s.contains("a")).count();
        long numBs = logData.filter(s->s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs  );

        session.stop();
    }
}
