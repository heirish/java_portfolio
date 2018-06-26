package com.company.platform.team.projspark;

import com.company.platform.team.projspark.data.AppParameters;
import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.modules.FastClustering;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
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

            SparkSession spark =  SparkSession.builder().appName("StructuredStream").getOrCreate();
            // https://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/structured-streaming-programming-guide.html
            // https://stackoverflow.com/questions/46153105/how-to-get-kafka-offsets-for-structured-query-for-manual-and-reliable-offset-man/46174353
            // https://www.jianshu.com/p/ae07471c1f8d
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
                    // 可以写回kafka或写到第三方统计平台
                    logger.debug("query processing: " + queryProgressEvent.progress());
                }

                @Override
                public void onQueryTerminated(QueryTerminatedEvent queryTerminatedEvent) {
                    logger.debug("query terminated: " + queryTerminatedEvent.id());
                }
            });

            Dataset<Row> df = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", appParameters.brokers)
                    .option("subscribe", appParameters.topics)
                    // .option("startingOffsets", "latest")
                    .load();

            // df.toJavaRDD().foreach();
            // df.foreach();
            // df.map();
            Dataset<String> dfLog = df.selectExpr("CAST(value as STRING)").as(Encoders.STRING());
            Dataset<String> dfLogWithLeafId = dfLog.map(new MapFunction<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    Map<String, String> fields = gson.fromJson(s, Map.class);
                    if (fields.containsKey(Constants.FIELD_BODY)
                            && fields.containsKey(Constants.FIELD_PROJECTNAME)) {
                        long start = System.nanoTime();
                        String body = fields.get(Constants.FIELD_BODY);
                        String projectName = fields.get(Constants.FIELD_PROJECTNAME);
                        String leafId = FastClustering.findCluster(projectName, body, 0, 0.3);
                        long end = System.nanoTime();
                        fields.put("leafId", leafId);
                        fields.put("processTimeCost",
                                String.format("%s", TimeUnit.NANOSECONDS.toMicros(end - start)));
                        //logger.info(String.format("Found cluster %s for log %s", leafId, body));
                        //logger.info(FastClustering.getPatternTreeString());
                        return gson.toJson(fields);
                    } else {
                        return s;
                    }
                }
            }, Encoders.STRING());

            StreamingQuery query = dfLogWithLeafId
                    .writeStream()
                    .format("json")
                    .option("checkpointLocation", "./checkpoint")
                    .option("path", "./output")
                    .outputMode("append")
                    .start();

            query.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void rddProcess(JavaRDD<ConsumerRecord<String, String>> messages) {
        if (messages.count() > 0) {
             messages.foreach( s -> {
                 logger.debug(s.value());
             });
            logger.debug("batch count: " + messages.count());
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
