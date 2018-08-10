package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.utils.*;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
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
public class PatternLeafAppenderBolt implements IRichBolt {
    private OutputCollector collector;
    private Map configMap;
    private boolean replayTuple;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.configMap = map;
        this.replayTuple = true;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(1);
            Map<String, String> logMap = GsonFactory.getGson().fromJson(log, Map.class);
            String projectName = logMap.get(Constants.FIELD_PROJECTNAME);
            String bodyTokenString = logMap.get(Constants.FIELD_PATTERNTOKENS);
            List<String> tokens = Arrays.asList(bodyTokenString.split(Constants.PATTERN_TOKENS_DELIMITER));

            Recognizer recognizer = Recognizer.getInstance(configMap);
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
            PatternNode node = new PatternNode(tokens);
            PatternNodeKey nodeKey = recognizer.addNode(levelKey, node);

            if (nodeKey == null) {
                logMap.put(Constants.FIELD_LEAFID, "");
            } else {
                Map<String, String> unmergedMap = new HashMap<>();
                unmergedMap.put(Constants.FIELD_PATTERNID, nodeKey.toString());
                unmergedMap.put(Constants.FIELD_PATTERNTOKENS, bodyTokenString);
                collector.emit(Constants.PATTERN_UNMERGED_STREAMID, new Values(GsonFactory.getGson().toJson(unmergedMap)));

                logMap.put(Constants.FIELD_LEAFID, nodeKey.getId());
            }

            // to es
            logMap.remove(Constants.FIELD_PATTERNTOKENS);
            collector.emit(Constants.LOG_OUT_STREAMID, new Values(GsonFactory.getGson().toJson(logMap)));

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
        outputFieldsDeclarer.declareStream(Constants.PATTERN_META_STREAMID, new Fields("value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
