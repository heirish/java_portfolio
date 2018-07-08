package com.company.platform.team.projspark.PatternCursoryFinder;

import com.company.platform.team.projspark.common.data.Constants;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import com.company.platform.team.projspark.common.preprocess.Preprocessor;
import com.google.gson.Gson;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
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

import static org.apache.spark.sql.functions.col;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class SparkFinder {
    private static final Gson gson = new Gson();

    //https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/sql/JavaSparkSQLExample.java
    //http://timepasstechies.com/spark-dataframe-split-one-column-multiple-columns-using-split-function/
    public static void startWork(FinderServiceConfigure conf, String appName) throws Exception {
        double leafSimilarity = conf.getLeafSimilarity();
        String logOutType = conf.getLogOutType();
        String logOutPath = conf.getLogOutPath();
        String logPatternOutType = conf.getLogPatternOutType();
        String logPatternOutPath = conf.getLogPatternOutPath();
        String projectName = conf.getProjectFilter();

        SparkSession spark = createSparkSession(appName);

        //Data input
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", conf.getKafkaBrokers())
                .option("subscribe", conf.getKafkaTopics())
                // .option("startingOffsets", "latest")
                .load();

        //processing
        Dataset<String> dfLog = df.selectExpr("CAST(value as STRING)").as(Encoders.STRING());
        List<StructField> columns = new ArrayList<>();
        columns.add(DataTypes.createStructField("projectName", DataTypes.StringType, false));
        columns.add(DataTypes.createStructField("log_content", DataTypes.StringType, false));
        columns.add(DataTypes.createStructField(Constants.FIELD_PATTERNID, DataTypes.StringType, true));
        columns.add(DataTypes.createStructField(Constants.FIELD_PATTERNTOKENS, DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(columns);
        Dataset<Row>dfLogWithLeafId = dfLog.map(new MapFunction<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                Map<String, String> fields = gson.fromJson(s, Map.class);
                String projectName = fields.get(Constants.FIELD_PROJECTNAME);
                if (fields != null && fields.containsKey(Constants.FIELD_BODY)) {
                    long start = System.nanoTime();
                    String body = fields.get(Constants.FIELD_BODY);
                    List<String> tokens = Preprocessor.transform(body);
                    PatternNodeKey nodeKey = PatternLeaves.getInstance().getParentNodeId(tokens, projectName,
                            1-leafSimilarity);
                    String leafId = nodeKey != null ? nodeKey.toString() : "";
                    long end = System.nanoTime();
                    fields.put(Constants.FIELD_LEAFID, leafId);

                    //for Test
                    fields.put("bodyLength", String.format("%s", body.length()));
                    fields.put("processTimeCost",
                            String.format("%s", TimeUnit.NANOSECONDS.toMicros(end - start)));
                    return RowFactory.create(projectName, gson.toJson(fields),
                            leafId, String.join(Constants.PATTERN_NODE_KEY_DELIMITER, tokens));
                } else {
                    return RowFactory.create(projectName, s, "", "");
                }
            }
        }, RowEncoder.apply(structType));

        // Data output downflow to index
        StreamingQuery queryIndex = dfLogWithLeafId
                .filter(col("projectName").eqNullSafe(projectName))
                .select("projectName", "log_content")
                .writeStream()
                .format("json")
                .option("checkpointLocation", logOutPath + "-checkpoint")
                .option("path", logOutPath)
                .outputMode("append")
                .start();

        // Data to pattern retriever;
        StreamingQuery queryPatternBase= dfLogWithLeafId
                .filter(col("projectName").eqNullSafe(projectName))
                //.selectExpr("CAST (patternId as STRING) as patternId", "CAST (bodyTokens as STRING) as bodyTokens")
                //already been string after mapFunction
                .select(Constants.FIELD_PATTERNID, Constants.FIELD_PATTERNTOKENS)
                .writeStream()
                .format("json")
                .option("checkpointLocation", logPatternOutPath + "-checkpoint")
                .option("path", logPatternOutPath)
                .outputMode("append")
                .start();

        //queryIndex.awaitTermination();
        queryPatternBase.awaitTermination();

    }

    private static SparkSession createSparkSession(String appName) {
        SparkSession spark =  SparkSession.builder().appName(appName).getOrCreate();
        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent queryStartedEvent) {
                //System.out.println("query started: " + queryStartedEvent.id());
            }

            @Override
            public void onQueryProgress(QueryProgressEvent queryProgressEvent) {
                SourceProgress[] sources = queryProgressEvent.progress().sources();
                for (SourceProgress source: sources) {
                    //System.out.println("start offset: " + source.startOffset());
                    //System.out.println("end offset: " + source.endOffset());
                }
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminatedEvent) {
                //System.out.println("query terminated: " + queryTerminatedEvent.id());
            }
        });
        return spark;
    }
}
