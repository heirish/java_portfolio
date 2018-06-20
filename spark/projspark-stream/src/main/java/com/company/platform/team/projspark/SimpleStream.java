package com.company.platform.team.projspark;

import com.company.platform.team.projspark.common.AppParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by admin on 2018/6/13.
 * bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      topic1,topic2
 */
public class SimpleStream{
    private static AppParameters appParameters = new AppParameters();
    private static final Pattern SPACE = Pattern.compile(" ");
    private static String logFile = "SimpleStream.log";
    private static PrintWriter logWriter;

    public static void main(String[] args) {
        try {
           logWriter = new PrintWriter(logFile, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Something goes wrong, will not write to log file.");
        }

        try{
            parseArgs(args);
            SparkConf sparkConf = new SparkConf().setAppName("StreamApplication");
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

            Set<String> topics = new HashSet<>(Arrays.asList(appParameters.topics.split(",")));
            Map<String, Object> kafkaParams = new HashMap<>();
            System.out.println(appParameters.brokers);
            System.out.println(topics);
            kafkaParams.put("bootstrap.servers", appParameters.brokers);
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "SimpleStream");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);

            JavaInputDStream<ConsumerRecord<String, String>> message = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics, kafkaParams)
            );

            // JavaDStream<String> lines = message.map(ConsumerRecord::value);
            // JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
            // JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2(1, 2));
            // JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1+i2);
            // wordCounts.print();

            // 可以看到数据实际是以micro-batch出现的，每一个rdd就是一个batch的数据
            message.foreachRDD(
                    rdd -> {
                        rddProcess(rdd);

                        OffsetRange[] offsetRanges = ((HasOffsetRanges)rdd.rdd()).offsetRanges();
                        rdd.foreachPartition(consumerRecords -> {
                            OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                            writeLog(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                        });
                        ((CanCommitOffsets) message.inputDStream()).commitAsync(offsetRanges);
                    }
            );

            jssc.start();
            jssc.awaitTermination();

            if (logWriter != null) {
                logWriter.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void writeLog(String log) {
       if (logWriter != null) {
           logWriter.println(log);
           logWriter.flush();
       } else {
           System.out.println(log);
       }
    }

    private static void rddProcess(JavaRDD<ConsumerRecord<String, String>> messages) {
        if (messages.count() > 0) {
             messages.foreach( s -> {
                 writeLog(s.value());
             });
            writeLog("batch count: " + messages.count());
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
