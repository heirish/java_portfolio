package com.company.platform.team.projpatternreco.stormtopology.refinder;

import com.company.platform.team.projpatternreco.stormtopology.leaffinder.PatternLeafSimilarity;
import com.company.platform.team.projpatternreco.stormtopology.utils.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.stormtopology.utils.PatternRecognizeException;
import com.google.gson.Gson;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternRefinerBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(PatternRefinerBolt.class);
    private static final Gson gson = new Gson();

    private OutputCollector collector;
    private Map redisConfMap;
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
        this.replayTuple = false; // for node not found, will never find it.
        this.redisConfMap = new HashMap<String, Object>();
        this.redisConfMap.putAll((Map)map.get("redis"));

        lastBackupTime = 0;
        backupInterval = 10 * 60 * 1000;
        //backupInterval = 6 * 1000;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(1);
            Map<String, String> logMap = gson.fromJson(log, Map.class);
            PatternNodeKey parentNodeKey = PatternNodeKey.fromString(logMap.get(Constants.FIELD_PATTERNID));
            List<String> patternTokens = Arrays.asList(logMap.get(Constants.FIELD_PATTERNTOKENS)
                    .split(Constants.PATTERN_TOKENS_DELIMITER));

            //merge i-1, add i
            for (int i = 1; i < 11; i++) {
                double decayedSimilarity = 1-PatternLeafSimilarity.getInstance(-1)
                        .getSimilarity(parentNodeKey.getProjectName())* Math.pow(1-decayRefactor, i);
                boolean isLastLevel = (i == 10) ? true : false;
                Pair<PatternNodeKey, List<String>> nextLevelTuple = PatternNodes.getInstance(redisConfMap)
                        .mergePatternToNode(parentNodeKey, patternTokens, 1- decayedSimilarity, isLastLevel);
                if (nextLevelTuple == null) {
                    break;
                }
                parentNodeKey = nextLevelTuple.getLeft();
                patternTokens = nextLevelTuple.getRight();
            }

            if (System.currentTimeMillis() - lastBackupTime > backupInterval) {
                //logger.info("back up pattern trees");
                //saveTreeToFile("tree/visualpatterntree", "");
                //backupTree("tree/patterntree", "");
                lastBackupTime = System.currentTimeMillis();
            }
            collector.ack(tuple);
        } catch (PatternRecognizeException pre) {
            collector.reportError(pre);
            if (replayTuple) {
                collector.fail(tuple);
            } else {
                collector.ack(tuple);
            }
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
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void saveTreeToFile(String fileName, String projectName) {
        try {
            File file = new File(fileName);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            FileWriter fw = new FileWriter(fileName);
            if (StringUtils.isEmpty(projectName)) {
                Set<String> projectNames = PatternNodes.getInstance(redisConfMap).getAllProjectsName();
                for (String name : projectNames) {
                    fw.write(PatternNodes.getInstance(redisConfMap).visualize(name));
                }
            } else {
                fw.write(PatternNodes.getInstance(redisConfMap).visualize(projectName));
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void backupTree(String fileName, String projectName) {
        try {
            File file = new File(fileName);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }

            FileWriter fw = new FileWriter(fileName);
            if (StringUtils.isEmpty(projectName)) {
                Set<String> projectNames = PatternNodes.getInstance(redisConfMap).getAllProjectsName();
                for (String name : projectNames) {
                    fw.write(PatternNodes.getInstance(redisConfMap).toString(name));
                }
            } else {
                fw.write(PatternNodes.getInstance(redisConfMap).toString(projectName));
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
