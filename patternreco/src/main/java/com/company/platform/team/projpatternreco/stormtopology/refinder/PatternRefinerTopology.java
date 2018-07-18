package com.company.platform.team.projpatternreco.stormtopology.refinder;

import clojure.lang.Cons;
import com.company.platform.team.projpatternreco.common.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.RunningType;
import com.company.platform.team.projpatternreco.stormtopology.leaffinder.PatternLeafFinderBolt;
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
public class PatternRefinerTopology {
    private static final Logger logger = Logger.getLogger(PatternRefinerTopology.class);
    private static String confFile;
    private static String topologyName;
    private static RunningType runningType;
    private PatternRecognizeConfigure config;

    public PatternRefinerTopology() {
        config = PatternRecognizeConfigure.getInstance(confFile, runningType);
    }

    private TopologyBuilder createTopologyBuilder() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //Spout kafka
        String unmergedLogTopic = config.getConfigMap("topics").get("unmergedLog").toString();
        String unmergedLogTopicSpout = unmergedLogTopic + "-spout";
        topologyBuilder.setSpout(unmergedLogTopicSpout,
                createKafkaSpout(unmergedLogTopic), config.getParallelismCount(unmergedLogTopicSpout));

        //Bolts
        String reducerBoltName = "UnmergedLogReducerBolt";
        topologyBuilder.setBolt(reducerBoltName, new UnmergedLogReducerBolt(60, 500),
                config.getParallelismCount(reducerBoltName))
                .shuffleGrouping(unmergedLogTopicSpout);
        String refinderBoltName = "PatternRefinerBolt";
        topologyBuilder.setBolt(reducerBoltName, new PatternRefinerBolt(config.getLeafSimilarity()),
                config.getParallelismCount(refinderBoltName))
                .fieldsGrouping(reducerBoltName, Constants.PATTERN_UNMERGED_STREAMID, new Fields(Constants.FIELD_PATTERNID));

        return topologyBuilder;
    }

    private void start() {
        TopologyBuilder tb = createTopologyBuilder();

        if (runningType == RunningType.CLUSTER) {
            logger.info("Starting Pattern Refiner CLUSTER topology");
            try {
                StormSubmitter.submitTopology(topologyName, config.getStormConfig(), tb.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
            logger.info("Started Pattern Refiner CLUSTER topology");
        } else {
            logger.info("Starting Pattern Refiner  LOCAL topology");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config.getStormConfig(), tb.createTopology());
            logger.info("Started Pattern Refiner LOCAL topology");
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

    public static void main(String[] args) {
        try {
            parseArgs(args);
            PatternRefinerTopology topology = new PatternRefinerTopology();
            topology.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void parseArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", false, "show help");
        options.addOption("c", "conf", true, "configure file");
        options.addOption("n", "name", true, "topology name");
        options.addOption("t", "runningtype", true, "running type");

        //CommandLineParser parser = new BasicParser();
        CommandLineParser parser = new GnuParser();
        CommandLine commands = parser.parse(options, args);
        if (commands.hasOption("h")) {
            showHelp(options);
        }

        if (commands.hasOption("c")) {
            confFile = commands.getOptionValue("c");
        } else {
            confFile = "PatternRefiner.json";
        }
        if (commands.hasOption("n")) {
            topologyName = commands.getOptionValue("n");
        } else {
            topologyName = "PatternRefinerTopology";
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
