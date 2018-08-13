package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.stormtopology.bolts.*;
import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.data.RunningType;
import com.company.platform.team.projpatternreco.stormtopology.utils.Recognizer;
import com.company.platform.team.projpatternreco.stormtopology.utils.TimerSpout;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;

/**
 * Created by admin on 2018/7/12.
 */
public final class PatternRecognizeTopology {
    private static final Logger logger = Logger.getLogger(PatternRecognizeTopology.class);

    private static String confFile;
    private PatternRecognizeConfigure config;
    private static String topologyName;
    private static RunningType runningType;

    public static void main(String[] args) {
        try {
            parseArgs(args);
            PatternRecognizeTopology topology = new PatternRecognizeTopology();
            topology.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public PatternRecognizeTopology() {
        config = PatternRecognizeConfigure.getInstance(confFile, runningType);
        Recognizer recognizer = Recognizer.getInstance(config.getStormConfig());
        recognizer.initMetas();
    }
    private void start() {
        TopologyBuilder tb = createTopologyBuilder();

        if (runningType == RunningType.CLUSTER) {
            logger.info("Starting Pattern Recognize CLUSTER topology");
            try {
                StormSubmitter.submitTopology(topologyName, config.getStormConfig(), tb.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
            logger.info("Started Pattern Recognize CLUSTER topology");
        } else {
            logger.info("Starting Pattern Recognize LOCAL topology");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config.getStormConfig(), tb.createTopology());
            logger.info("Started Pattern Recognize LOCAL topology");
        }
    }

    private TopologyBuilder createTopologyBuilder() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //for metaUpdated
        String metaTopic = config.getConfigMap("topics").get("metaupdated").toString();
        String metaTopicSpout = metaTopic + "-spout";
        topologyBuilder.setSpout(metaTopicSpout,
                createKafkaSpout(metaTopic), config.getParallelismCount(metaTopicSpout));
        String metaUpdatedBoltName = "MetaUpdatedBolt";
        topologyBuilder.setBolt(metaTopic, new MetaUpdatedBolt(),
                config.getParallelismCount(metaUpdatedBoltName))
                .allGrouping(metaTopicSpout);

        // for LeafFinder
        String normalLogTopic = config.getConfigMap("topics").get("normalLog").toString();
        String normalLogTopicSpout = normalLogTopic + "-spout";
        topologyBuilder.setSpout(normalLogTopicSpout,
                createKafkaSpout(normalLogTopic), config.getParallelismCount(normalLogTopicSpout));
        String leafFinderBoltName = "PatternLeafFinderBolt";
        String leafAppenderBoltName = "PatternLeafAppenderBolt";
        String logIndexBoltName = "LogIndexBolt";
        topologyBuilder.setBolt(leafFinderBoltName, new PatternLeafFinderBolt(),
                config.getParallelismCount(leafFinderBoltName))
                .shuffleGrouping(normalLogTopicSpout);
        topologyBuilder.setBolt(leafAppenderBoltName, new PatternLeafAppenderBolt(),
                config.getParallelismCount(leafAppenderBoltName))
                .fieldsGrouping(leafFinderBoltName, Constants.PATTERN_UNADDED_STREAMID, new Fields(Constants.FIELD_PROJECTNAME));
        topologyBuilder.setBolt(logIndexBoltName, new LogIndexBolt(), config.getParallelismCount(logIndexBoltName))
                .shuffleGrouping(leafFinderBoltName, Constants.LOG_OUT_STREAMID)
                .shuffleGrouping(leafAppenderBoltName, Constants.LOG_OUT_STREAMID);

        //Finder output to kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBrokerHosts());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getProducerSerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getProducerSerializer());
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getFetchSizeBytes());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getFetchSizeBytes());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.company.platform.team.projpatternreco.stormtopology.utils.RoundRobinPartitioner");

        //// create KafkaBolt for unmerged logs
        String outputTopic = config.getConfigMap("topics").get("unmergedLog").toString();
        KafkaBolt outputKafkaBolt = new KafkaBolt().withTopicSelector(new DefaultTopicSelector(outputTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "value"))
                .withProducerProperties(props);
        topologyBuilder.setBolt(outputTopic + "Bolt", outputKafkaBolt,
                config.getParallelismCount(outputTopic+"Bolt"))
                .shuffleGrouping(leafFinderBoltName, Constants.PATTERN_UNMERGED_STREAMID)
                .shuffleGrouping(leafAppenderBoltName, Constants.PATTERN_UNMERGED_STREAMID);


        //for pattern refiner
        String unmergedLogTopic = config.getConfigMap("topics").get("unmergedLog").toString();
        String unmergedLogTopicSpout = unmergedLogTopic + "-spout";
        topologyBuilder.setSpout(unmergedLogTopicSpout,
                createKafkaSpout(unmergedLogTopic), config.getParallelismCount(unmergedLogTopicSpout));
        String reducerBoltName = "UnmergedLogReducerBolt";
        String refinerBoltName = "PatternRefinerBolt";
        topologyBuilder.setBolt(reducerBoltName, new UnmergedLogReducerBolt(),
                config.getParallelismCount(reducerBoltName))
                .shuffleGrouping(unmergedLogTopicSpout);
        topologyBuilder.setBolt(refinerBoltName, new PatternRefinerBolt(),
                config.getParallelismCount(refinerBoltName))
                .fieldsGrouping(reducerBoltName, Constants.PATTERN_UNMERGED_STREAMID, new Fields(Constants.FIELD_PROJECTNAME));

        // for flush nodes to DB
        String timerSpout = "timer-spout";
        topologyBuilder.setSpout(timerSpout, new TimerSpout(), 1);
        String timerSpreadBolt = "TimerSpreadBolt";
        String redisFlushBolt = "RedisFlushBolt";
        topologyBuilder.setBolt(timerSpreadBolt, new TimerSpreadBolt(), 1)
                .shuffleGrouping(timerSpout);
        topologyBuilder.setBolt(redisFlushBolt, new RedisFlushBolt(), config.getParallelismCount(redisFlushBolt))
                .fieldsGrouping(timerSpreadBolt, Constants.REDIS_FLUSH_STREAMID, new Fields(Constants.FIELD_PROJECTNAME));

        return topologyBuilder;
    }
    private KafkaSpout createKafkaSpout(String topicName) {
        KafkaSpoutConfig.Builder<String, String> kafkaSpoutBuilder = KafkaSpoutConfig
                .builder(config.getKafkaBrokerHosts(), topicName)
                .setGroupId(this.topologyName)
                .setRecordTranslator(new ByTopicRecordTranslator<>((r)
                        -> new Values(r.value(), r.topic(), r.partition(), r.offset(), r.key())
                        , new Fields("value", "topic", "partition", "offset", "key")))
                .setFirstPollOffsetStrategy(config.getOffsetStrategy())
                .setMaxPartitionFectchBytes(config.getFetchSizeBytes());
        return new KafkaSpout<>(kafkaSpoutBuilder.build());
    }

    private static void parseArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("f", "file", true, "configure file");
        options.addOption("n", "name", true, "topology name");
        options.addOption("t", "runningtype", true, "running type");

        CommandLineParser parser = new DefaultParser();
        CommandLine commands = parser.parse(options, args);
        if (commands.hasOption("h")) {
            showHelp(options);
        }

        // -c storm reserved option
        if (commands.hasOption("f")) {
            confFile = commands.getOptionValue("f");
        } else {
            confFile = "PatternRecognize-com.json";
            logger.info("Using default config file: [" + confFile + "].");
        }
        if (commands.hasOption("n")) {
            topologyName = commands.getOptionValue("n");
        } else {
            topologyName = "PatternRecognizeTopology";
            logger.info("Using default topology name: [" + topologyName+ "].");
        }
        if (commands.hasOption("t")) {
            runningType = RunningType.fromString(commands.getOptionValue("t"));
        } else {
            runningType = RunningType.LOCAL;
        }
    }

    private static void showHelp(Options options) {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("PatternRecognizeTopology", options);
        System.exit(0);
    }
}
