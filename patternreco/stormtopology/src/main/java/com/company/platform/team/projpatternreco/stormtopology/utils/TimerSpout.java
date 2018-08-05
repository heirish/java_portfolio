package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by Administrator on 2018/8/4 0004.
 */
public class TimerSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private long ticFlushRedis;
    private long intervalFlushRedis;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        this.ticFlushRedis = 0;
        this.intervalFlushRedis = 2000;

    }

    @Override
    public void nextTuple() {
        Utils.sleep(1);
        if (this.ticFlushRedis > this.intervalFlushRedis) {
            collector.emit(new Values("redisFlush"));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.TIMER_STREAMID, new Fields("value"));
    }
}
