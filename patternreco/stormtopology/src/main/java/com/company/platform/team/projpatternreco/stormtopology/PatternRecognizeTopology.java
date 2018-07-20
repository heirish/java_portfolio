package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.leaffinder.LogIndexBolt;
import com.company.platform.team.projpatternreco.stormtopology.leaffinder.PatternLeafAppenderBolt;
import com.company.platform.team.projpatternreco.stormtopology.leaffinder.PatternLeafFinderBolt;
import com.company.platform.team.projpatternreco.stormtopology.refinder.PatternRefinerBolt;
import com.company.platform.team.projpatternreco.stormtopology.refinder.UnmergedLogReducerBolt;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by admin on 2018/7/12.
 */
//TODO:need a db access for PatternLevelTree
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
    }

    private TopologyBuilder createTopologyBuilder() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();


        // for LeafFinder
        String normalLogTopic = config.getConfigMap("topics").get("normalLog").toString();
        String crashLogTopic = config.getConfigMap("topics").get("crashLog").toString();
        String normalLogTopicSpout = normalLogTopic + "-spout";
        String crashLogTopicSpout = crashLogTopic + "-spout";
        topologyBuilder.setSpout(normalLogTopicSpout,
                createKafkaSpout(normalLogTopic), config.getParallelismCount(normalLogTopicSpout));
        //topologyBuilder.setSpout(crashLogTopic,
        //        createKafkaSpout(crashLogTopic), config.getParallelismCount(crashLogTopicSpout));

        String leafFinderBoltName = "PatternLeafFinderBolt";
        String leafAppenderBoltName = "PatternLeafAppenderBolt";
        String logIndexBoltName = "LogIndexBolt";
        topologyBuilder.setBolt(leafFinderBoltName, new PatternLeafFinderBolt(config.getLeafSimilarity()),
                config.getParallelismCount(leafFinderBoltName))
                .shuffleGrouping(normalLogTopicSpout);
               // .shuffleGrouping(crashLogTopic);
        topologyBuilder.setBolt(leafAppenderBoltName, new PatternLeafAppenderBolt(), config.getParallelismCount(leafAppenderBoltName))
                .fieldsGrouping(leafFinderBoltName, Constants.PATTERN_UNADDED_STREAMID, new Fields(Constants.FIELD_PROJECTNAME));
        topologyBuilder.setBolt(logIndexBoltName, new LogIndexBolt(), config.getParallelismCount(logIndexBoltName))
                .shuffleGrouping(leafFinderBoltName, Constants.LOG_OUT_STREAMID)
                .shuffleGrouping(leafAppenderBoltName, Constants.LOG_OUT_STREAMID);

        //output cursorFinder to kafka or else where
        //Properties props = new Properties();
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBrokerHosts());
        //props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getProducerSerializer());
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getProducerSerializer());
        //props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getFetchSizeBytes());
        //props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getFetchSizeBytes());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.naver.nelo.symbolicator.bolt.RoundRobinPartitioner");

        //// create KafkaBolt
        //String outputTopic = config.getConfigMap("topics").get("logWithPatternId").toString();
        //KafkaBolt outputKafkaBolt = new KafkaBolt().withTopicSelector(new DefaultTopicSelector(outputTopic))
        //        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "value"))
        //        .withProducerProperties(props);
        //topologyBuilder.setBolt(outputTopic + "Bolt", outputKafkaBolt,
        //        config.getParallelismCount(outputTopic+"Bolt"))
        //        .shuffleGrouping(leafFinderBoltName, Constants.PATTERN_UNMERGED_STREAMID)
        //        .shuffleGrouping(leafAppenderBoltName, Constants.PATTERN_UNMERGED_STREAMID);


        //for pattern refiner
        String unmergedLogTopic = config.getConfigMap("topics").get("unmergedLog").toString();
        String unmergedLogTopicSpout = unmergedLogTopic + "-spout";
        topologyBuilder.setSpout(unmergedLogTopicSpout,
                createKafkaSpout(unmergedLogTopic), config.getParallelismCount(unmergedLogTopicSpout));

        String reducerBoltName = "UnmergedLogReducerBolt";
        String refinerBoltName = "PatternRefinerBolt";
        topologyBuilder.setBolt(reducerBoltName, new UnmergedLogReducerBolt(60, 500),
                config.getParallelismCount(reducerBoltName))
                .shuffleGrouping(unmergedLogTopicSpout);
        topologyBuilder.setBolt(reducerBoltName, new PatternRefinerBolt(config.getLeafSimilarity(), 0.9),
                config.getParallelismCount(refinerBoltName))
                .fieldsGrouping(reducerBoltName, Constants.PATTERN_UNMERGED_STREAMID, new Fields(Constants.FIELD_PROJECTNAME));

        return topologyBuilder;
    }

    private void start() {
        TopologyBuilder tb = createTopologyBuilder();

        if (runningType == RunningType.CLUSTER) {
            logger.info("Starting Pattern Leaf Finder CLUSTER topology");
            try {
                StormSubmitter.submitTopology(topologyName, config.getStormConfig(), tb.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
            logger.info("Started Pattern Leaf Finder CLUSTER topology");
        } else {
            logger.info("Starting Pattern Leaf Finder LOCAL topology");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config.getStormConfig(), tb.createTopology());
            logger.info("Started Pattern Leaf Finder LOCAL topology");
        }
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
        options.addOption("c", "conf", true, "configure file");
        options.addOption("n", "name", true, "topology name");
        options.addOption("t", "runningtype", true, "running type");

        CommandLineParser parser = new DefaultParser();
        CommandLine commands = parser.parse(options, args);
        if (commands.hasOption("h")) {
            showHelp(options);
        }

        if (commands.hasOption("c")) {
            confFile = commands.getOptionValue("c");
        } else {
            confFile = "PatternRecognize.json";
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
        formater.printHelp("PatternRefinerTopology", options);
        System.exit(0);
    }
}
