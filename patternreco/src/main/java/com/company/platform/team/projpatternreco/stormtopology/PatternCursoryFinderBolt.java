package com.company.platform.team.projpatternreco.stormtopology;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternCursoryFinderBolt implements IRichBolt {
    private OutputCollector collector;
    private Gson gson;
    private boolean replayTuple;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        gson = new Gson();
        replayTuple = true;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = gson.fromJson(log, Map.class);

            String projectName = logMap.get(Constants.FIELD_PROJECTNAME);
            String body = logMap.get(Constants.FIELD_BODY);
            PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
            List<String> bodyTokens = Preprocessor.transform(body);
            PatternNodeKey nodeKey = PatternNodesCenter.getInstance()
                    .getParentNodeId(bodyTokens, levelKey, 0.3);

            logMap.put(Constants.FIELD_LEAFID, nodeKey.toString());
            collector.emit(Constants.LOG_OUT_STREAMID, new Values(logMap));

            Map<String, String> unmergedMap = new HashMap<>();
            unmergedMap.put(Constants.FIELD_PATTERNID, nodeKey.toString());
            unmergedMap.put(Constants.FIELD_PATTERNTOKENS, String.join(Constants.PATTERN_TOKENS_DELIMITER, bodyTokens));
            collector.emit(Constants.PATTERN_UNMERGED_STREAMID, new Values(unmergedMap));
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
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
