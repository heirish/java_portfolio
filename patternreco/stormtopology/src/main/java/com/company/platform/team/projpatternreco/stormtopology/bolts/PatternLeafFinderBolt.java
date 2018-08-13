package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.stormtopology.utils.CommonUtil;
import com.company.platform.team.projpatternreco.stormtopology.utils.GsonFactory;
import com.company.platform.team.projpatternreco.stormtopology.utils.Recognizer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternLeafFinderBolt implements IRichBolt {
    private OutputCollector collector;
    private Recognizer recognizer;

    private boolean replayTuple;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.recognizer = Recognizer.getInstance(map);
        this.replayTuple = CommonUtil.replayFailedTuple(map);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = GsonFactory.getGson().fromJson(log, Map.class);
            String projectName = logMap.get(Constants.FIELD_PROJECTNAME);
            String body = logMap.get(Constants.FIELD_BODY);
            if (!StringUtils.isBlank(body)) {
                Pair<PatternNodeKey, List<String>> result = recognizer.getLeafNodeId(projectName, body);

                PatternNodeKey nodeKey = result.getLeft();
                String tokenString = String.join(Constants.PATTERN_TOKENS_DELIMITER, result.getRight());
                if (nodeKey == null) { // to leafaddbolt
                    logMap.put(Constants.FIELD_PATTERNTOKENS, tokenString);
                    collector.emit(Constants.PATTERN_UNADDED_STREAMID,
                            new Values(projectName, GsonFactory.getGson().toJson(logMap)));
                } else {
                    // to kafka
                    Map<String, String> valueMap = new HashMap<>();
                    valueMap.put(Constants.FIELD_PATTERNID, nodeKey.toString());
                    valueMap.put(Constants.FIELD_PATTERNTOKENS, tokenString);
                    collector.emit(Constants.PATTERN_UNMERGED_STREAMID,
                            new Values(GsonFactory.getGson().toJson(valueMap)));

                    // to es
                    logMap.put(Constants.FIELD_LEAFID, nodeKey.getId());
                    collector.emit(Constants.LOG_OUT_STREAMID,
                            new Values(GsonFactory.getGson().toJson(logMap)));
                }
            }
            collector.ack(tuple);
        } catch (Exception e) {
            collector.reportError(e);
            if (replayTuple) {
                collector.fail(tuple);
            }
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
}
