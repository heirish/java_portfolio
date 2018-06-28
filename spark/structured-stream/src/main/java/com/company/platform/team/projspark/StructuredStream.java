package com.company.platform.team.projspark;

import com.company.platform.team.projspark.data.AppParameters;
import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.modules.FastClustering;
import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.commons.cli.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin on 2018/6/13.
 * spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
 * --class com.company.platform.team.projspark.StructuredStream target\original-structued-stream-1.0-SNAPSHOT.jar \
 * -b broker1-host:port,broker2-host:port -t topic1,topic2\
 */
public class StructuredStream{
    private static AppParameters appParameters = new AppParameters();
    private static final Logger logger = Logger.getLogger("");
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        try{
            parseArgs(args);

            SparkSession spark = createSparkSession("StructuredStream");

            //Data input
            Dataset<Row> df = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", appParameters.brokers)
                    .option("subscribe", appParameters.topics)
                    // .option("startingOffsets", "latest")
                    .load();

            //processing
            Dataset<String> dfLog = df.selectExpr("CAST(value as STRING)").as(Encoders.STRING());
            Dataset<String> dfLogWithLeafId = dfLog.map(new MapFunction<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    return findCluster(s);
                }
            }, Encoders.STRING());

            // Data output downflow to index
            StreamingQuery queryIndex = dfLogWithLeafId
                    .writeStream()
                    .format("json")
                    .option("checkpointLocation", "./outputcheckpoint")
                    .option("path", "./output")
                    .outputMode("append")
                    .start();

            // Data to pattern retriever;
            StreamingQuery queryPatternBase= dfLogWithLeafId
                    .writeStream()
                    .format("json")
                    .option("checkpointLocation", "./patternbasecheckpoint")
                    .option("path", "./patternbase")
                    .outputMode("append")
                    .start();

            queryIndex.awaitTermination();
            queryPatternBase.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static SparkSession createSparkSession(String appName) {
        SparkSession spark =  SparkSession.builder().appName(appName).getOrCreate();
        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent queryStartedEvent) {
                logger.debug("query started: " + queryStartedEvent.id());
            }

            @Override
            public void onQueryProgress(QueryProgressEvent queryProgressEvent) {
                SourceProgress[] sources = queryProgressEvent.progress().sources();
                for (SourceProgress source: sources) {
                    logger.debug("start offset: " + source.startOffset());
                    logger.debug("end offset: " + source.endOffset());
                }
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminatedEvent) {
                logger.debug("query terminated: " + queryTerminatedEvent.id());
            }
        });
        return spark;
    }

    private static String findCluster(String log) throws Exception{
        Map<String, String> fields = gson.fromJson(log, Map.class);
        if (fields.containsKey(Constants.FIELD_BODY)
                && fields.containsKey(Constants.FIELD_PROJECTNAME)) {
            long start = System.nanoTime();
            String body = fields.get(Constants.FIELD_BODY);
            String projectName = fields.get(Constants.FIELD_PROJECTNAME);
            String leafId = FastClustering.findCluster(projectName, body, 0, 0.3);
            long end = System.nanoTime();
            fields.put("leafId", leafId);

            //for Test
            fields.put("bodyLength", String.format("%s", body.length()));
            fields.put("processTimeCost",
                    String.format("%s", TimeUnit.NANOSECONDS.toMicros(end - start)));
            return gson.toJson(fields);
        } else {
            return log;
        }
    }

    private static void parseArgs(String[] args){
        Options options  = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("b", "brokers", true, "brokers");
        options.addOption("t", "topics", true, "topics");

        CommandLineParser parser = new BasicParser();
        try{
            CommandLine commands = parser.parse(options, args);
            if (commands.hasOption("h")) {
                showHelp(options);
            }
            if (commands.hasOption("b")) {
                appParameters.brokers = commands.getOptionValue("b");
            }
            if (commands.hasOption("t")) {
                appParameters.topics = commands.getOptionValue("t");
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
