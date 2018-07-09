package com.company.platform.team.projspark;

import com.company.platform.team.projspark.PatternCursoryFinder.FinderService;
import com.company.platform.team.projspark.PatternCursoryFinder.FinderServiceConfigure;
import com.company.platform.team.projspark.PatternNodeHelper.PatternNodeCenterType;
import com.company.platform.team.projspark.PatternNodeHelper.PatternNodeServer;
import com.company.platform.team.projspark.PatternRefiner.RefinerService;
import com.company.platform.team.projspark.PatternRefiner.RefinerServiceConfigure;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by admin on 2018/6/13.
 * spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
 * --class com.company.platform.team.projspark.StructuredStream target\original-structued-stream-1.0-SNAPSHOT.jar \
 * -b broker1-host:port,broker2-host:port -t topic1,topic2\
 */
public class PatternRecognize{
    private static final Logger logger = Logger.getLogger("");
    private static String jobType;
    private static String confFile;

    public static void main(String[] args) {
        try {
            parseArgs(args);

            if (StringUtils.equalsIgnoreCase(jobType, "spark")) {
                startPatternFinderService(confFile);
            } else {
                startPatternRefinerService(confFile);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startPatternFinderService(String jsonConfFile) throws Exception{
        FinderServiceConfigure conf = FinderServiceConfigure.parseFromJson(jsonConfFile);
        FinderService finderService = new FinderService(conf, "logpatternfinder");
        finderService.run();
    }

    private static void startPatternRefinerService(String jsonConfFile) throws Exception {
         new PatternNodeServer("localhost:7911",
                    PatternNodeCenterType.HDFS, 1).start();
         try {
            Thread.sleep(3000);
        } catch (InterruptedException x) {
        }

        RefinerServiceConfigure conf = RefinerServiceConfigure.parseFromJson(jsonConfFile);
        RefinerService refinerService = new RefinerService(conf, "patternretriever");
        refinerService.run();
    }


    private static void parseArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("j", "job", true, "job type");
        options.addOption("c", "conf", true, "configure file");

        //CommandLineParser parser = new BasicParser();
        CommandLineParser parser = new GnuParser();
        CommandLine commands = parser.parse(options, args);
        if (commands.hasOption("h")) {
            showHelp(options);
        }

        if (commands.hasOption("j")) {
            jobType = commands.getOptionValue("j");
        }

        if (commands.hasOption("c")) {
            confFile = commands.getOptionValue("c");
        }
    }

    private static void showHelp(Options options) {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("PatternRecognize", options);
        System.exit(0);
    }
}
