package com.company.platform.team.projpatternreco.stormtopology.leaffinder;

import com.company.platform.team.projpatternreco.common.data.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.preprocess.Preprocessor;
import com.google.gson.Gson;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public PatternLeafFinderBolt(double leafSimilarity) {
        this.leafSimilarity = leafSimilarity;
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
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
            List<String> bodyTokens = Preprocessor.transform(logMap.get(Constants.FIELD_BODY));
            PatternNodeKey nodeKey = PatternLeaves.getInstance(redisConfMap)
                    .getParentNodeId(bodyTokens, levelKey, 1 - leafSimilarity,
                            Constants.FINDCLUSTER_TOLERANCE_TIMES);

            String tokenString = String.join(Constants.PATTERN_TOKENS_DELIMITER, bodyTokens);
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
