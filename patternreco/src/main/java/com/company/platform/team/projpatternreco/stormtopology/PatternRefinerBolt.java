package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.google.gson.Gson;
import edu.emory.mathcs.backport.java.util.Arrays;
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

import static com.company.platform.team.projpatternreco.sparkandhadoop.patternerfiner.MapReduceRefinerWorker.retrievePattern;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternRefinerBolt implements IRichBolt {
    private OutputCollector collector;
    private Gson gson;
    private boolean replayTuple;
    private int patternLevel;
    private double maxDist;

    public PatternRefinerBolt(int level, double leafSimlarity) {
        this.patternLevel = level;
        this.gson = new Gson();
        this.replayTuple = true;
        this.maxDist = 1 - leafSimlarity * Math.pow(leafSimlarity, level+1);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = gson.fromJson(log, Map.class);
            String parentNodeId = logMap.get(Constants.FIELD_PATTERNID);
            List<String> patternTokens = Arrays.asList(logMap.get(Constants.FIELD_PATTERNTOKENS)
                    .split(Constants.PATTERN_NODE_KEY_DELIMITER));

            PatternNodeKey parentNodeKey = PatternNodeKey.fromString(parentNodeId);
            PatternNode parentNode = PatternLevelTree.getInstance().getNode(parentNodeKey);
            List<String> mergedTokens = retrievePattern(parentNode.getPatternTokens(), patternTokens);
            parentNode.updatePatternTokens(mergedTokens);
            PatternNodeKey grandNodeKey = null;
            if (!parentNode.hasParent()) {
                parentNodeKey = PatternLevelTree.getInstance()
                        .getParentNodeId(mergedTokens, parentNodeKey.getProjectName(), parentNodeKey.getLevel()+1, maxDist);
                parentNode.setParent(grandNodeKey);
            }
            grandNodeKey = parentNode.getParentId();
            //update the tree node(parent Id and pattern) by key
            PatternLevelTree.getInstance().updateNode(parentNodeKey, parentNode);

            Map<String, String> unmergedMap = new HashMap<>();
            unmergedMap.put(Constants.FIELD_PATTERNID, grandNodeKey.toString());
            unmergedMap.put(Constants.FIELD_PATTERNTOKENS, String.join(Constants.PATTERN_NODE_KEY_DELIMITER,mergedTokens));
            collector.emit("", new Values(unmergedMap));
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
        outputFieldsDeclarer.declareStream("", new Fields("value"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
