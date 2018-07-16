package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.google.gson.Gson;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        this.replayTuple = true;
        this.maxDist = 1 - leafSimlarity * Math.pow(leafSimlarity, level+1);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.gson = new Gson();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = gson.fromJson(log, Map.class);
            String parentNodeId = logMap.get(Constants.FIELD_PATTERNID);
            List<String> patternTokens = Arrays.asList(logMap.get(Constants.FIELD_PATTERNTOKENS)
                    .split(Constants.PATTERN_TOKENS_DELIMITER));

            PatternNodeKey parentNodeKey = PatternNodeKey.fromString(parentNodeId);
            Pair<PatternNodeKey, List<String>> nextLevelTuple = PatternNodes.getInstance()
                    .mergePatternToNode(parentNodeKey, patternTokens, maxDist);

            if (this.patternLevel == 10) {
                saveTreeToFile("tree/visualpatterntree", "");
                backupTree("tree/patterntree");
            }
            Map<String, String> unmergedMap = new HashMap<>();
            unmergedMap.put(Constants.FIELD_PATTERNID, nextLevelTuple.getLeft().toString());
            unmergedMap.put(Constants.FIELD_PATTERNTOKENS, String.join(
                    Constants.PATTERN_TOKENS_DELIMITER,nextLevelTuple.getRight()));
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
        outputFieldsDeclarer.declareStream(Constants.PATTERN_UNMERGED_STREAMID, new Fields("value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void saveTreeToFile(String fileName, String projectName) {
        try {
            FileWriter fw = new FileWriter(fileName);
            String treeString = StringUtils.isEmpty(projectName)
                    ? PatternNodes.getInstance().visualize()
                    : PatternNodes.getInstance().visualize(projectName);
            System.out.println(treeString);
            fw.write(treeString);
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void backupTree(String fileName) {
        try {
            FileWriter fw = new FileWriter(fileName);
                try {
                    fw.write(PatternNodes.getInstance().toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
