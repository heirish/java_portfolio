package com.company.platform.team.proj;

import com.company.platform.team.proj.common.AppPrameters;
import org.apache.commons.cli.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
/**
 * Created by admin on 2018/6/13.
 */
public class SimpleApp {
    private static AppPrameters appParameters = new AppPrameters();

    public static void main(String[] args) {
        try{
            parseArgs(args);
            System.out.println(appParameters.inputDataFile);
            SparkSession session = SparkSession.builder().appName("Simple Application").getOrCreate();
            Dataset<String> logData = session.read().textFile(appParameters.inputDataFile).cache();

            long numAs = logData.filter(s->s.contains("a")).count();
            long numBs = logData.filter(s->s.contains("b")).count();

            System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs  );
            session.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void parseArgs(String[] args){
        Options options  = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("f", "file", true, "input file");

        CommandLineParser parser = new BasicParser();
        try{
            CommandLine commands = parser.parse(options, args);
            if (commands.hasOption("h")) {
                showHelp(options);
            }
            if (commands.hasOption("f")) {
                appParameters.inputDataFile= commands.getOptionValue("f");
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private static void showHelp(Options options) {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("SimpleApp", options);
        System.exit(0);
    }
}
