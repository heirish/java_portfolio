package com.company.platform.team.projpatternreco.stormtopology.refinder;

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
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternRefinerBolt implements IRichBolt {
    private OutputCollector collector;
    private Gson gson;
    private boolean replayTuple;
    private double leafSimilarity;
    private double decayRefactor;

    //@Test
    private long lastBackupTime;
    private long backupInterval;

    public PatternRefinerBolt(double leafSimlarity, double decayRefacotr) {
        this.leafSimilarity = leafSimlarity;
        this.decayRefactor = decayRefacotr;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.gson = new Gson();
        this.replayTuple = true;

        lastBackupTime = System.currentTimeMillis();
        backupInterval = 10 * 60 * 1000;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = gson.fromJson(log, Map.class);
            PatternNodeKey parentNodeKey = PatternNodeKey.fromString(logMap.get(Constants.FIELD_PATTERNID));
            List<String> patternTokens = Arrays.asList(logMap.get(Constants.FIELD_PATTERNTOKENS)
                    .split(Constants.PATTERN_TOKENS_DELIMITER));

            for (int i=1; i<10; i++) {
                double maxDist = 1 - leafSimilarity * Math.pow(decayRefactor, i);
                Pair<PatternNodeKey, List<String>> nextLevelTuple = PatternNodes.getInstance()
                        .mergePatternToNode(parentNodeKey, patternTokens, maxDist);
                if (nextLevelTuple == null) {
                   break;
                }
                parentNodeKey = nextLevelTuple.getLeft();
                patternTokens = nextLevelTuple.getRight();
            }

            if (System.currentTimeMillis() - lastBackupTime > backupInterval) {
                saveTreeToFile("tree/visualpatterntree", "");
                backupTree("tree/patterntree");
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
