package com.company.platform.team.projpatternreco.stormtopology.refinder;

import com.company.platform.team.projpatternreco.common.data.Constants;
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

/**
 * Created by admin on 2018/7/12.
 */
public class UnmergedLogReducerBolt implements IRichBolt {
    private static final Gson gson  =  new Gson();

    private OutputCollector collector;
    private boolean replayTuple;
    private long maxCachedPatterns;
    private long cacheInterval;
    private long cacheStartTime;
    private Map<PatternNodeKey, List<String>> cachedPatterns;

    public UnmergedLogReducerBolt(long cacheIntervalSeconds, long maxCachedPatterns) {
       this.maxCachedPatterns = maxCachedPatterns;
       this.cacheInterval = cacheIntervalSeconds * 1000;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.replayTuple = true;
        this.cachedPatterns = new HashMap<>();
        this.cacheStartTime = 0L;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = gson.fromJson(log, Map.class);
            PatternNodeKey nodeKey = PatternNodeKey.fromString(logMap.get(Constants.FIELD_PATTERNID));
            List<String> patternTokens = Arrays.asList(logMap.get(Constants.FIELD_PATTERNTOKENS)
                    .split(Constants.PATTERN_TOKENS_DELIMITER));

            if (cachedPatterns.containsKey(nodeKey)) {
                List<String> newTokens = Aligner.retrievePattern(cachedPatterns.get(nodeKey), patternTokens);
                cachedPatterns.put(nodeKey, newTokens);
            } else {
                cachedPatterns.put(nodeKey, patternTokens);
            }

            if ((System.currentTimeMillis() - cacheStartTime) >= cacheInterval
                    || cachedPatterns.size() >= maxCachedPatterns) {
               for (Map.Entry<PatternNodeKey, List<String>> entry : cachedPatterns.entrySet()) {
                   String tokenString = String.join(Constants.PATTERN_TOKENS_DELIMITER, entry.getValue());
                   collector.emit(Constants.PATTERN_UNMERGED_STREAMID,
                           new Values(entry.getKey().getProjectName().toString(), entry.getKey(), tokenString));
               }
               cachedPatterns = new HashMap<>();
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
        outputFieldsDeclarer.declareStream(Constants.PATTERN_UNMERGED_STREAMID,
                new Fields(Constants.FIELD_PROJECTNAME, Constants.FIELD_PATTERNID, "value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
