package com.company.platform.team.projpatternreco.sparkandhadoop;

import com.company.platform.team.projpatternreco.sparkandhadoop.patterncursoryfinder.FinderService;
import com.company.platform.team.projpatternreco.sparkandhadoop.patterncursoryfinder.ServiceType;
import com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper.PatternNodeCenterType;
import com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper.PatternNodeServer;
import com.company.platform.team.projpatternreco.sparkandhadoop.patternerfiner.RefinerService;
import com.company.platform.team.projpatternreco.sparkandhadoop.patternerfiner.RefinerServiceConfigure;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.commons.cli.*;

/**
 * Created by admin on 2018/6/13.
 * spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
 * --class com.company.platform.team.pro.sparkandhadoop.StructuredStream target\original-structued-stream-1.0-SNAPSHOT.jar \
 * -b broker1-host:port,broker2-host:port -t topic1,topic2\
 */
public class PatternRecognize{
    private static final Logger logger = Logger.getLogger("");
    private static String jobType;
    private static String confFile;

    public static void main(String[] args) {
        try {
            parseArgs(args);

            if (StringUtils.equalsIgnoreCase(jobType, "hadoop")) {
                startPatternRefinerService(confFile);
            } else {
                startPatternFinderService(confFile, jobType);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startPatternFinderService(String jsonConfFile, String serviceType) throws Exception{
        FinderService finderService = new FinderService("logpatternfinder", ServiceType.fromString(serviceType), jsonConfFile);
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
