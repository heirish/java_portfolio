package com.company.platform.team.projspark;

import com.company.platform.team.projspark.data.AppParameters;
import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.modules.FastClustering;
import com.company.platform.team.projspark.preprocess.Preprocessor;
import com.company.platform.team.projspark.utils.FluentScheduledExecutorService;
import com.company.platform.team.projspark.utils.PatternRetrieveTask;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.commons.cli.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
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
        try {
            parseArgs(args);
            if (StringUtils.equalsIgnoreCase(appParameters.jobType, "spark")) {
                startSparkWork();
            } else {
                startScheduledHadoopWork(1, 5, TimeUnit.SECONDS);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/sql/JavaSparkSQLExample.java
    //http://timepasstechies.com/spark-dataframe-split-one-column-multiple-columns-using-split-function/
    private static void startSparkWorkd1() throws Exception {
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
        List<StructField> columns = new ArrayList<>();
        columns.add(DataTypes.createStructField("log_content", DataTypes.StringType, false));
        columns.add(DataTypes.createStructField(Constants.FIELD_PATTERNID, DataTypes.StringType, false));
        columns.add(DataTypes.createStructField(Constants.FIELD_PATTERNTOKENS, DataTypes.StringType, false));
        StructType structType = DataTypes.createStructType(columns);
        Dataset<Row>dfLogWithLeafId = dfLog.map(new MapFunction<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(s, )
            }
        }, Encoders.javaSerialization(Row.class));

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

    }

    private static void startSparkWork() throws Exception{
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
        //Dataset<String> dfLogWithLeafId = dfLog.map(new MapFunction<String, String>() {
        //    @Override
        //    public String call(String s) throws Exception {
        //        return findCluster(s);
        //    }
        //}, Encoders.STRING());
        Dataset<Row>dfLogWithLeafId = dfLog.map(new MapFunction<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(s, "id");
                //return findCluster(s);
            }
        }, Encoders.javaSerialization(Row.class));

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

    }

    private static void startScheduledHadoopWork(long initialDelay,
                                                 long period, TimeUnit unit) throws Exception{
        new FluentScheduledExecutorService(1)
                .scheduleWithFixedDelay(new PatternRetrieveTask(appParameters),
                        initialDelay, period, unit);

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
        if (fields != null && fields.containsKey(Constants.FIELD_BODY)
                && fields.containsKey(Constants.FIELD_PROJECTNAME)) {
            long start = System.nanoTime();
            String body = fields.get(Constants.FIELD_BODY);
            String projectName = fields.get(Constants.FIELD_PROJECTNAME);
            String leafId = FastClustering.findCluster(projectName, 0,
                    Preprocessor.transform(body), 0.3);
            //TODO:
            //if (nodeLevel > 0) {
            //  setParent();
            //}
            //
            long end = System.nanoTime();
            fields.put(Constants.FIELD_LEAFID, leafId);

            //for Test
            fields.put("bodyLength", String.format("%s", body.length()));
            fields.put("processTimeCost",
                    String.format("%s", TimeUnit.NANOSECONDS.toMicros(end - start)));
            return gson.toJson(fields);
        } else {
            return log;
        }
    }

    private static void parseArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("j", "job", true, "job type");
        options.addOption("b", "brokers", true, "brokers");
        options.addOption("t", "topics", true, "topics");
        options.addOption("i", "input",true, "input dir");
        options.addOption("r", "inputregex",true, "input dir regex filter");
        options.addOption("o", "output", true, "output dir");

        //CommandLineParser parser = new BasicParser();
        CommandLineParser parser = new GnuParser();
        CommandLine commands = parser.parse(options, args);
        if (commands.hasOption("h")) {
            showHelp(options);
        }

        if (commands.hasOption("j")) {
            appParameters.jobType = commands.getOptionValue("j");
        }

        if (commands.hasOption("b")) {
            appParameters.brokers = commands.getOptionValue("b");
        }
        if (commands.hasOption("t")) {
            appParameters.topics = commands.getOptionValue("t");
        }

        if (commands.hasOption("i")) {
            appParameters.inputDir = commands.getOptionValue("i");
        }
        if (commands.hasOption("r")) {
            appParameters.inputfilter= commands.getOptionValue("r");
        }
        if (commands.hasOption("o")) {
            appParameters.outputDir = commands.getOptionValue("o");
        }


        if (StringUtils.equalsIgnoreCase(appParameters.jobType, "spark")) {
            logger.info("brokers " + appParameters.brokers);
            logger.info("topics " + appParameters.topics);
            if (StringUtils.isEmpty(appParameters.brokers)
                    || StringUtils.isEmpty(appParameters.topics)) {
                throw new Exception("spark work should have brokers and topics parameter");
            }
        } else {
            if (StringUtils.isEmpty(appParameters.inputDir)
                    || StringUtils.isEmpty(appParameters.outputDir)) {
                throw new Exception("hadoop work should have inputdir and outputdir parameter");
            }
        }
    }

    private static void showHelp(Options options) {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("SimpleApp", options);
        System.exit(0);
    }
}
