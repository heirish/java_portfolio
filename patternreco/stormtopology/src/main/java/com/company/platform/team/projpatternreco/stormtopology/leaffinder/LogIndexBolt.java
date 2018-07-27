package com.company.platform.team.projpatternreco.stormtopology.leaffinder;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by admin on 2018/7/12.
 */
public class LogIndexBolt implements IRichBolt {
    private OutputCollector collector;
    private boolean replayTuple;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.replayTuple = true;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            //TODO:index to es
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
}
