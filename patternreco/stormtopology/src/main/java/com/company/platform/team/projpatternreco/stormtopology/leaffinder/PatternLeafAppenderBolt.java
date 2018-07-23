package com.company.platform.team.projpatternreco.stormtopology.leaffinder;

import clojure.lang.Cons;
import com.company.platform.team.projpatternreco.common.data.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternLeafAppenderBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(PatternLeafAppenderBolt.class);
    private static final Gson gson = new Gson();

    private OutputCollector collector;
    private boolean replayTuple;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        replayTuple = true;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log= tuple.getString(1);
            Map<String, String> logMap = gson.fromJson(log, Map.class);

            String projectName = logMap.get(Constants.FIELD_PROJECTNAME);
            String bodyTokenString = logMap.get(Constants.FIELD_PATTERNTOKENS);
            List<String> tokens = Arrays.asList(bodyTokenString.split(Constants.PATTERN_TOKENS_DELIMITER));
            PatternNodeKey nodeKey = PatternLeaves.getInstance().addNewLeaf(projectName, tokens);

            if (nodeKey == null) {
                logMap.put(Constants.FIELD_LEAFID, "");
            } else  {
                Map<String, String> unmergedMap = new HashMap<>();
                unmergedMap.put(Constants.FIELD_PATTERNID, nodeKey.toString());
                unmergedMap.put(Constants.FIELD_PATTERNTOKENS, bodyTokenString);
                collector.emit(Constants.PATTERN_UNMERGED_STREAMID, new Values(gson.toJson(unmergedMap)));

                logMap.put(Constants.FIELD_LEAFID, nodeKey.toString());
            }

            // to es
            logMap.remove(Constants.FIELD_PATTERNTOKENS);
            collector.emit(Constants.LOG_OUT_STREAMID, new Values(gson.toJson(logMap)));
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
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
