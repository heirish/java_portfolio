package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.utils.GsonFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.index.IndexRequestBuilder;

import java.util.Map;

/**
 * Created by admin on 2018/7/12.
 */
public class LogIndexBolt extends Indexer {
    @Override
    public void execute(Tuple tuple) {
        try {
            String log = tuple.getString(0);
            if (StringUtils.isEmpty(log)) {
                //Fields("value", "topic", "partition", "offset", "key")))
                logger.info("Empty_tuple:" + ReflectionToStringBuilder.toString(tuple));
                collector.ack(tuple);
                return;
            }

            Map<String, Object> logMap = GsonFactory.getGson().fromJson(log, Map.class);
            if (logMap.containsKey(Constants.FIELD_PROJECTNAME)) {
                String projectName = logMap.get(Constants.FIELD_PROJECTNAME).toString();
                if (isAllowedProject(projectName)) {
                    String id = null;
                    if (logMap.containsKey(FIELD_ID)) {
                        id = logMap.get(FIELD_ID).toString();
                    }
                    removeSystemFields(logMap);
                    IndexRequestBuilder request = client.prepareIndex(getIndex(logMap), projectName, id)
                            .setSource(logMap);

                    request.execute().actionGet();
                }
            }
            collector.ack(tuple);
        } catch (Exception e) {
            collector.reportError(e);
            logger.error("Failed to process tuple : " + tuple.getString(0), e);
            if (isKnownFailure(e.toString())) {
                collector.ack(tuple);
                logger.debug("[ACK] skip processing tuple because this is know failure cases");
            } else {
                collector.fail(tuple);
                logger.debug("[FAIL] failed 1 log");
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
