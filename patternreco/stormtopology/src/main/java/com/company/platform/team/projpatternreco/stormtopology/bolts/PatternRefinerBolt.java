package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.utils.*;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.google.gson.Gson;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternRefinerBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(PatternRefinerBolt.class);
    private static final Gson gson = new Gson();

    private OutputCollector collector;
    private Map redisConfMap;
    private boolean replayTuple;
    private double leafSimilarity;
    private double decayFactor;

    //@Test
    private long lastBackupTime;
    private long backupInterval;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.replayTuple = false; // for node not found, will never find it.

        lastBackupTime = 0;
        backupInterval = 10 * 60 * 1000;
        //backupInterval = 6 * 1000;
        parseConfig(map);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(1);
            Map<String, String> logMap = gson.fromJson(log, Map.class);
            PatternNodeKey parentNodeKey = PatternNodeKey.fromString(logMap.get(Constants.FIELD_PATTERNID));
            List<String> patternTokens = Arrays.asList(logMap.get(Constants.FIELD_PATTERNTOKENS)
                    .split(Constants.PATTERN_TOKENS_DELIMITER));

            //merge i-1, add i
            Recognizer nodesUtilInstance = Recognizer.getInstance(redisConfMap);
            PatternMetas metasInstance = PatternMetas.getInstance(redisConfMap);
            String projectName = parentNodeKey.getProjectName();
            int levelMax = metasInstance.getPatternLevelMax(projectName);
            for (int i = 1; i < levelMax + 1; i++) {
                double similarity = metasInstance.getLeafSimilarity(projectName);
                double decayedSimilarity = 1 - similarity * Math.pow(1-decayFactor, i);
                boolean isLastLevel = (i == 10) ? true : false;
                Pair<PatternNodeKey, List<String>> nextLevelTuple = nodesUtilInstance
                        .mergePatternToNode(parentNodeKey, patternTokens, 1- decayedSimilarity, isLastLevel);
                if (nextLevelTuple == null) {
                    break;
                }
                parentNodeKey = nextLevelTuple.getLeft();
                patternTokens = nextLevelTuple.getRight();
            }

            if (System.currentTimeMillis() - lastBackupTime > backupInterval) {
                //logger.info("back up pattern trees");
                //saveTreeToFile("tree/visualpatterntree", "");
                //backupTree("tree/patterntree", "");
                lastBackupTime = System.currentTimeMillis();
            }
            collector.ack(tuple);
        } catch (PatternRecognizeException pre) {
            collector.reportError(pre);
            if (replayTuple) {
                collector.fail(tuple);
            } else {
                collector.ack(tuple);
            }
        } catch (Exception e) {
            collector.reportError(e);
            collector.ack(tuple);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    private void parseConfig(Map map) {
        try {
            this.leafSimilarity = Double.parseDouble(((Map)map.get(Constants.CONFIGURE_PATTERNRECO_SECTION)).get("leafSimilarity").toString());
        } catch (Exception e) {
            this.leafSimilarity = Constants.PATTERN_LEAF_SIMILARITY_DEFAULT;
            logger.error("get leafSimilarity value from config file failed, use default value: " + this.leafSimilarity);
        }
        try {
            this.decayFactor = Double.parseDouble(((Map)map.get(Constants.CONFIGURE_PATTERNRECO_SECTION)).get("decayFactor").toString());
        } catch (Exception e) {
            this.decayFactor= Constants.SIMILARITY_DECAY_FACTOR_DEFAULT;
            logger.error("get decayFactor value from config file failed, use default value: " + this.decayFactor);
        }

        this.redisConfMap = new HashMap<String, Object>();
        this.redisConfMap.putAll((Map)map.get(Constants.CONFIGURE_REDIS_SECTION));
    }
}
