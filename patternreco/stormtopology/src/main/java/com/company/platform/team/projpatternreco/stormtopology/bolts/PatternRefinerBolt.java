package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.utils.*;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternRefinerBolt implements IRichBolt {
    private OutputCollector collector;
    private Map configMap;
    private boolean replayTuple;

    //@Test
    private long lastBackupTime;
    private long backupInterval;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.configMap = map;
        this.replayTuple = CommonUtil.replayFailedTuple(map);

        lastBackupTime = 0;
        backupInterval = 10 * 60 * 1000;
        //backupInterval = 6 * 1000;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(1);
            Map<String, String> logMap = GsonFactory.getGson().fromJson(log, Map.class);
            PatternNodeKey parentNodeKey = PatternNodeKey.fromString(logMap.get(Constants.FIELD_PATTERNID));
            List<String> patternTokens = Arrays.asList(logMap.get(Constants.FIELD_PATTERNTOKENS)
                    .split(Constants.PATTERN_TOKENS_DELIMITER));

            Recognizer recognizer = Recognizer.getInstance(configMap);
            recognizer.mergeTokenToNode(parentNodeKey, patternTokens);

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
}
