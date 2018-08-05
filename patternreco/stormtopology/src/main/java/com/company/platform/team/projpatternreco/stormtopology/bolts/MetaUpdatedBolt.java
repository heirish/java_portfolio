package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.data.PatternMetaType;
import com.company.platform.team.projpatternreco.stormtopology.data.RedisNodeCenter;
import com.company.platform.team.projpatternreco.stormtopology.utils.GsonFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2018/8/3.
 */
public class MetaUpdatedBolt implements IRichBolt {
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
            Map<String, String> logMap = GsonFactory.getGson().fromJson(log, Map.class);
            String projectName = logMap.get(Constants.FIELD_PROJECTNAME);
            String metaType = logMap.get(Constants.FIELD_META_TYPE);
            PatternMetaType type = PatternMetaType.fromString(metaType);

            //TODO:synchronize change meta from DB to redis
            String value = "";
            RedisNodeCenter redisNodeCenter = RedisNodeCenter.getInstance(redisConfMap);
            redisNodeCenter.setMetaData(projectName, type.toString(), value);
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
