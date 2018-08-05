package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.utils.GsonFactory;
import com.company.platform.team.projpatternreco.stormtopology.utils.Recognizer;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Administrator on 2018/8/4 0004.
 */
public class RedisFlushBolt implements IRichBolt{
    private OutputCollector collector;
    private Map configMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.configMap = map;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String message = tuple.getString(0);
            //会很耗时
            if (StringUtils.equals(message, "redisFlush")) {
                saveNodesFromRedisToDB();
            }

        } catch (Exception e) {
            collector.reportError(e);
        }
        collector.ack(tuple);
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

    //TODO: Flush redis to DB
    private void saveNodesFromRedisToDB() {
        //get all projects List
        Recognizer recognizer = Recognizer.getInstance(configMap);
        Set<String> projects = recognizer.getAllProjects();
        for (String projectName : projects) {
            //delete DB level 1-10
            //flush all level 1-10
            // if this is a new tree, relink level 0, and then set it to old tree.

            checkLeafCapacity(projectName, recognizer);
        }
    }

    private void checkLeafCapacity(String projectName, Recognizer recognizer) {
        if (projectName != null && recognizer != null) {
            if (recognizer.isLeafFull(projectName)) {
                //change similarity
                recognizer.stepDownLeafSimilarity(projectName);
                Map<String, String> valueMap = new HashMap<>();
                valueMap.put(Constants.FIELD_PROJECTNAME, projectName);
                valueMap.put(Constants.FIELD_META_TYPE, "similarity");
                collector.emit(Constants.PATTERN_META_STREAMID,
                        new Values(GsonFactory.getGson().toJson(valueMap)));

                //delete nodecenter and local
                recognizer.rebuildTree(projectName);
                //TODO:mask this project's tree as new, set a node's id to -1 for mask usage
            }
        }

    }
}
