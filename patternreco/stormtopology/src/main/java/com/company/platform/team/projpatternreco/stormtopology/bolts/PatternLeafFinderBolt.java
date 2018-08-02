package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.utils.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.preprocess.Preprocessor;
import com.company.platform.team.projpatternreco.stormtopology.utils.PatternMetas;
import com.company.platform.team.projpatternreco.stormtopology.utils.Recognizer;
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
    private Map redisConfMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.replayTuple = true;
        parseConfig(map);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = gson.fromJson(log, Map.class);
            String projectName = logMap.get(Constants.FIELD_PROJECTNAME);
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);

            String body = logMap.get(Constants.FIELD_BODY);
            if (!StringUtils.isBlank(body)) {
                Recognizer nodesUtilInstance = Recognizer.getInstance(redisConfMap);
                PatternMetas metaInstance = PatternMetas.getInstance(redisConfMap);

                List<String> tokens = preprocess(body,
                                                 metaInstance.getBodyLengthMax(projectName),
                                                 metaInstance.getTokensCountMax(projectName));
                double similarity = metaInstance.getLeafSimilarity(projectName);
                PatternNodeKey nodeKey = nodesUtilInstance.getParentNodeId(tokens, levelKey,
                        1 - similarity, Constants.FINDCLUSTER_TOLERANCE_TIMES);

                String tokenString = String.join(Constants.PATTERN_TOKENS_DELIMITER, tokens);
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
            collector.ack(tuple);
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

    private void parseConfig(Map map) {
        this.redisConfMap = new HashMap<String, Object>();
        this.redisConfMap.putAll((Map)map.get(Constants.CONFIGURE_REDIS_SECTION));
    }

    private List<String> preprocess(String body, int bodyLengthMax, int tokenCountMax) {
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
        return tokensLeft;
    }
}
