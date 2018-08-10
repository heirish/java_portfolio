package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.utils.Aligner;
import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.stormtopology.utils.GsonFactory;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by admin on 2018/7/12.
 */
public class UnmergedLogReducerBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(UnmergedLogReducerBolt.class);

    private OutputCollector collector;

    private boolean replayTuple;
    private Map<String, List<String>> cachedPatterns;
    private long maxCachedPatterns;
    private long cacheInterval;
    private long cacheStartTime;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        //for test
        this.replayTuple = false;
        //this.replayTuple = true;
        this.cachedPatterns = new HashMap<>();
        this.cacheStartTime = 0L;
        parseConfig(map);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = GsonFactory.getGson().fromJson(log, Map.class);
            PatternNodeKey nodeKey = PatternNodeKey.fromString(logMap.get(Constants.FIELD_PATTERNID));
            List<String> patternTokens = Arrays.asList(logMap.get(Constants.FIELD_PATTERNTOKENS)
                    .split(Constants.PATTERN_TOKENS_DELIMITER));

            if (cachedPatterns.containsKey(nodeKey.toString())) {
                List<String> newTokens = Aligner.retrievePattern(cachedPatterns.get(nodeKey.toString()), patternTokens);
                cachedPatterns.put(nodeKey.toString(), newTokens);
            } else {
                cachedPatterns.put(nodeKey.toString(), patternTokens);
            }

            if ((System.currentTimeMillis() - cacheStartTime) >= cacheInterval
                    || cachedPatterns.size() >= maxCachedPatterns) {
                for(Iterator<Map.Entry<String, List<String>>> it = cachedPatterns.entrySet().iterator();
                    it.hasNext(); ) {
                   Map.Entry<String, List<String>> entry = it.next();
                   PatternNodeKey entryNodeKey = PatternNodeKey.fromString(entry.getKey());
                   String tokenString = String.join(Constants.PATTERN_TOKENS_DELIMITER, entry.getValue());
                   Map<String, String> valueMap = new HashMap<>();
                   valueMap.put(Constants.FIELD_PATTERNID, entryNodeKey.toString());
                   valueMap.put(Constants.FIELD_PATTERNTOKENS, tokenString);
                   collector.emit(Constants.PATTERN_UNMERGED_STREAMID,
                           new Values(nodeKey.getProjectName(), GsonFactory.getGson().toJson(valueMap)));
                   it.remove();
               }
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
                new Fields(Constants.FIELD_PROJECTNAME, "value"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void parseConfig(Map map) {
        try {
            this.cacheInterval = (long)Double.parseDouble(((Map)map.get(Constants.CONFIGURE_PATTERNRECO_SECTION)).get("leafPatternCacheSeconds").toString());
        } catch (Exception e) {
            this.cacheInterval = Constants.LEAF_PATTERN_CACHE_SECONDS_DEFAULT * 1000;
            logger.error("get leafPatternCacheSeconds value from config file failed, use default value: " + this.cacheInterval / 1000 + "seconds.");
        }
        try {
            this.maxCachedPatterns = (long) Double.parseDouble((((Map)map.get(Constants.CONFIGURE_PATTERNRECO_SECTION)).get("leafPatternCacheCount").toString()));
        } catch (Exception e) {
            this.maxCachedPatterns = Constants.LEAF_PATTERN_CACHE_MAX_DEFAULT;
            logger.error("get leafPatternCacheCount value from config file failed, use default value: " + this.maxCachedPatterns);
        }
    }
}
