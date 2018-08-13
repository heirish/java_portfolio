package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.utils.Recognizer;
import com.company.platform.team.projpatternreco.stormtopology.utils.RedisUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Created by admin on 2018/8/6.
 */
public class TimerSpreadBolt implements IRichBolt{
    private static Logger logger = LoggerFactory.getLogger(TimerSpreadBolt.class);
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
            //spread by projectName
            if (StringUtils.equals(message, "redisFlush")) {
                RedisUtil redisUtil = RedisUtil.getInstance((Map)configMap.get(Constants.CONFIGURE_REDIS_SECTION));
                Set<String> projects = redisUtil.getAllProjects();
                for (String projectName : projects) {
                    logger.info("project: " + projectName + " emited.");
                    collector.emit(Constants.REDIS_FLUSH_STREAMID, new Values(projectName));
                }
            }
        } catch (Exception e) {
            logger.error("Timer spread error.", e);
        }
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Constants.REDIS_FLUSH_STREAMID,
                new Fields(Constants.FIELD_PROJECTNAME));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
