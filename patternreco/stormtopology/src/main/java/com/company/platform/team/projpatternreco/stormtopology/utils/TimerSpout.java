package com.company.platform.team.projpatternreco.stormtopology.utils;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Administrator on 2018/8/4 0004.
 */
public class TimerSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private long lastFlushRedisTime;
    private long intervalFlushRedis;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        this.lastFlushRedisTime = 0;
        this.intervalFlushRedis = 1 * 60 * 1000;
    }

    @Override
    public void nextTuple() {
        if (System.currentTimeMillis() - this.lastFlushRedisTime > this.intervalFlushRedis) {
            this.lastFlushRedisTime = System.currentTimeMillis();
            collector.emit(new Values("redisFlush"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("default", new Fields("value"));
    }
}
