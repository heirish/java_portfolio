package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.eventbus.EventBus;
import com.company.platform.team.projpatternreco.stormtopology.eventbus.SimilarityEvent;
import com.company.platform.team.projpatternreco.stormtopology.utils.Constants;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2018/8/3.
 */
public class MetaUpdatedBolt implements IRichBolt {
    private static final Gson gson = new Gson();
    private static final Logger logger = LoggerFactory.getLogger(UnmergedLogReducerBolt.class);
    private static final EventBus eventBus = EventBus.getInstance();

    private OutputCollector collector;

    private Map redisConfMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        this.redisConfMap = new HashMap<String, Object>();
        this.redisConfMap.putAll((Map)map.get(Constants.CONFIGURE_REDIS_SECTION));
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = gson.fromJson(log, Map.class);
            String projectName = logMap.get(Constants.FIELD_PROJECTNAME);
            String metaType = logMap.get(Constants.FIELD_META_TYPE);

            if (StringUtils.equals(metaType, "similarity")) {
                SimilarityEvent event = new SimilarityEvent();
                event.setProjectName(projectName);
                eventBus.publish(event);
            } else {

            }
            collector.ack(tuple);
        } catch (Exception e) {
            collector.reportError(e);
            collector.fail(tuple);
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
