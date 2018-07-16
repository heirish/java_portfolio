package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.preprocess.Preprocessor;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.List;

/**
 * Created by admin on 2018/7/16.
 */
public class ComputeCPULoadTest {
    private static final String log = "Kafka Spout opened with the following configuration: KafkaSpoutConfig{kafkaProps\u003d{enable.auto.commit\u003dfalse, group.id\u003dcrash-collector, max.partition.fetch.bytes\u003d47185920, bootstrap.servers\u003d10.113.120.196:9092,10.113.124.201:9092,10.113.126.217:9092,10.113.120.195:9092,10.113.122.218:9092}, key\u003dorg.apache.kafka.common.serialization.StringDeserializer@ddaa3cd, value\u003dorg.apache.kafka.common.serialization.StringDeserializer@7559bbc4, pollTimeoutMs\u003d200, offsetCommitPeriodMs\u003d30000, maxUncommittedOffsets\u003d10000000, firstPollOffsetStrategy\u003dUNCOMMITTED_LATEST, subscription\u003dorg.apache.storm.kafka.spout.NamedSubscription@66103ab5, translator\u003dorg.apache.storm.kafka.spout.ByTopicRecordTranslator@2312b813, retryService\u003dKafkaSpoutRetryExponentialBackoff{delay\u003dTimeInterval{length\u003d0, timeUnit\u003dSECONDS}, ratio\u003dTimeInterval{length\u003d2, timeUnit\u003dMILLISECONDS}, maxRetries\u003d2147483647, maxRetryDelay\u003dTimeInterval{length\u003d10, timeUnit\u003dSECONDS}}}";
    private static final String projectName = "nelo2-monitoring-alpha";
    private static final double leafSimilarity = 0.9;

    public static void main(String[] args) {
        preparePatternTree();
        long startTime = System.currentTimeMillis();
        doComputation();
        long endTime = System.currentTimeMillis();
        System.out.println("done in " + (endTime - startTime) + " ms");
    }

    private static void preparePatternTree() {
        PatternNodeKey nodeKey = new PatternNodeKey(projectName, 0);
        List<String> tokens = Preprocessor.transform(log);
        PatternNode node = new PatternNode(tokens);
        PatternNodes.getInstance().addNode(nodeKey, node, 0);
    }

    private static void doComputation() {
        List<String> tokens = Preprocessor.transform(log);
        //System.out.println(Arrays.toString(tokens.toArray()));
        PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
        PatternNodeKey nodeKey = PatternNodes.getInstance().getParentNodeId(tokens, levelKey, 1-leafSimilarity);
        //System.out.println(nodeKey.toString());
        Pair<PatternNodeKey, List<String>> umerged = PatternNodes.getInstance().mergePatternToNode(nodeKey, tokens,
                1 - leafSimilarity * Math.pow(leafSimilarity, 1));
    }
}
