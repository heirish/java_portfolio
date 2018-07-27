package com.company.platform.team.projpatternreco.stormtopology.leaffinder;

import com.company.platform.team.projpatternreco.stormtopology.utils.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.preprocess.Preprocessor;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternLeafFinderBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(PatternLeafFinderBolt.class);
    private static final Gson gson = new Gson();

    private OutputCollector collector;
    private boolean replayTuple;
    private double leafSimilarity;
    private Map redisConfMap;
    private int bodyLengthMax;
    private int tokenCountMax;

    public PatternLeafFinderBolt(double leafSimilarity, int bodyLengthMax, int tokenCountMax) {
        this.leafSimilarity = leafSimilarity;
        this.bodyLengthMax = bodyLengthMax;
        this.tokenCountMax = tokenCountMax;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        replayTuple = true;

        this.redisConfMap = new HashMap<String, Object>();
        this.redisConfMap.putAll((Map)map.get("redis"));
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = gson.fromJson(log, Map.class);

            String projectName = logMap.get(Constants.FIELD_PROJECTNAME);
            //test
            //if (!StringUtils.equals(projectName, "Band_android")) {
            //    collector.ack(tuple);
            //}
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
            String body = logMap.get(Constants.FIELD_BODY);
            if (!StringUtils.isBlank(body)) {
                if (body.length() > bodyLengthMax) {
                    logger.info("log body is too long, the max length we can handle is " + bodyLengthMax + ",will eliminate the exceeded");
                    body = body.substring(0, bodyLengthMax);
                }
                List<String> bodyTokens = Preprocessor.transform(body);

                List<String> tokensLeft = bodyTokens;
                if (bodyTokens.size() > tokenCountMax) {
                    logger.warn("sequenceLeft exceeds the max length we can handle, will eliminate the exceeded part to "
                            + Constants.IDENTIFY_EXCEEDED_TYPE + ",tokens:" + Arrays.toString(tokensLeft.toArray()));
                    tokensLeft = new ArrayList(bodyTokens.subList(0, tokenCountMax - 1));
                    tokensLeft.add(Constants.IDENTIFY_EXCEEDED_TYPE);
                }

                double projectLeafSimilarity = PatternLeafSimilarity.getInstance(leafSimilarity).getSimilarity(projectName);
                PatternNodeKey nodeKey = PatternLeaves.getInstance(redisConfMap)
                        .getParentNodeId(tokensLeft, levelKey, 1-projectLeafSimilarity,
                                Constants.FINDCLUSTER_TOLERANCE_TIMES);

                String tokenString = String.join(Constants.PATTERN_TOKENS_DELIMITER, tokensLeft);
                if (nodeKey == null) { // to leafaddbolt
                    logMap.put(Constants.FIELD_PATTERNTOKENS, tokenString);
                    collector.emit(Constants.PATTERN_UNADDED_STREAMID, new Values(projectName, gson.toJson(logMap)));
                } else {
                    // to kafka
                    Map<String, String> valueMap = new HashMap<>();
                    valueMap.put(Constants.FIELD_PATTERNID, nodeKey.toString());
                    valueMap.put(Constants.FIELD_PATTERNTOKENS, tokenString);
                    collector.emit(Constants.PATTERN_UNMERGED_STREAMID, new Values(gson.toJson(valueMap)));

                    // to es
                    logMap.put(Constants.FIELD_LEAFID, nodeKey.toString());
                    collector.emit(Constants.LOG_OUT_STREAMID, new Values(gson.toJson(logMap)));
                }
            }
            collector.ack(tuple);
        } catch (Exception e) {
            collector.reportError(e);
            if (replayTuple) {
                collector.fail(tuple);
            } else {
                collector.ack(tuple);
            }
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.LOG_OUT_STREAMID, new Fields("value"));
        outputFieldsDeclarer.declareStream(Constants.PATTERN_UNMERGED_STREAMID, new Fields("value"));
        outputFieldsDeclarer.declareStream(Constants.PATTERN_UNADDED_STREAMID, new Fields(Constants.FIELD_PROJECTNAME,"value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
