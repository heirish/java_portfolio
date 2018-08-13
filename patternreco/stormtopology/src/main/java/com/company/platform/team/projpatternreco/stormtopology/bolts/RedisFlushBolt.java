package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.utils.Recognizer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by Administrator on 2018/8/4 0004.
 */
public class RedisFlushBolt implements IRichBolt{
    private static Logger logger = LoggerFactory.getLogger(RedisFlushBolt.class);
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
            String projectName= tuple.getString(0);
            Recognizer recognizer = Recognizer.getInstance(configMap);
            long startTime = System.currentTimeMillis();
            recognizer.flushNodesFromRedisToMysql(projectName);
            long endTime = System.currentTimeMillis();
            logger.info("flushNodesToMysql used: " + (endTime - startTime) + " ms.");
            startTime = System.currentTimeMillis();
            recognizer.relinkProjectLeaves(projectName);
            endTime = System.currentTimeMillis();
            logger.info("relinkLeaves used: " + (endTime - startTime) + " ms.");
            startTime = System.currentTimeMillis();
            recognizer.limitLeafCapacity(projectName);
            endTime = System.currentTimeMillis();
            logger.info("limitLeafCapacity used: " + (endTime - startTime) + " ms.");
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
}
