package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.data.DBProject;
import com.company.platform.team.projpatternreco.stormtopology.data.PatternMetaType;
import com.company.platform.team.projpatternreco.stormtopology.utils.MysqlUtil;
import com.company.platform.team.projpatternreco.stormtopology.utils.RedisUtil;
import com.company.platform.team.projpatternreco.stormtopology.utils.GsonFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by admin on 2018/8/3.
 */
public class MetaUpdatedBolt implements IRichBolt {
    private static Logger logger = LoggerFactory.getLogger(MetaUpdatedBolt.class);

    private OutputCollector collector;

    private Map conf;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.conf = map;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            Map<String, String> logMap = GsonFactory.getGson().fromJson(log, Map.class);
            String projectName = logMap.get(Constants.FIELD_PROJECTNAME);
            String metaType = logMap.get(Constants.FIELD_META_TYPE);
            PatternMetaType type = null;
            try {
                type = PatternMetaType.fromString(metaType);
            } catch (Exception e) {
                logger.error("Invalid meta type string: ", metaType);
            }

            if (projectName != null && type != null) {
                RedisUtil redisUtil = RedisUtil.getInstance((Map)conf.get(Constants.CONFIGURE_REDIS_SECTION));
                MysqlUtil mysqlUtil = MysqlUtil.getInstance((Map)conf.get(Constants.CONFIGURE_MYSQL_SECTION));
                DBProject project = mysqlUtil.getProjectMeta(projectName);
                switch (type) {
                    case PROJECT_ID:
                        redisUtil.setMetaData(projectName, PatternMetaType.PROJECT_ID.getTypeString(),
                                String.valueOf(project.getId()));
                        break;
                    case LEAF_NODES_LIMIT:
                        redisUtil.setMetaData(projectName, PatternMetaType.LEAF_NODES_LIMIT.getTypeString(),
                                String.valueOf(project.getLeafMax()));
                        break;
                    default:
                        logger.info("Invalid meta type string: ", metaType);
                }
            }
        } catch (Exception e) {
            logger.error("Meta updated error.", e);
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
