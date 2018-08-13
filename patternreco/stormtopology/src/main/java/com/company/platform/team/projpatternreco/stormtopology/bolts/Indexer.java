package com.company.platform.team.projpatternreco.stormtopology.bolts;

import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.utils.CommonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class Indexer implements Serializable, IRichBolt {
    static final Logger logger = LoggerFactory.getLogger("indexer");
    static Client client;
    static final String FIELD_LOGTIME = "logTime";
    static final String FIELD_ROUTING = "@routing";
    static final String FIELD_LIMIT = "@limiting";
    static final String FIELD_ID = "@id";

    OutputCollector collector;
    private String indexPrefix;
    private List<String> knownFailureList;
    private Set<String> allowedProjects;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        Map<String, Object> esConf = (Map<String, Object>) stormConf.get("elasticsearch");
        this.collector = outputCollector;
        this.indexPrefix = (String) esConf.getOrDefault("indexPrefix", "log-");
        try {
            prepareElasticSearch(stormConf);
        } catch (UnknownHostException e) {
            logger.error("failed to initialize elasticsearch client", e);
        }
        this.knownFailureList = (List<String>) (esConf.get("knownFailures"));

        //for test
        this.allowedProjects = new HashSet<>();
        this.allowedProjects.add("Band");
        this.allowedProjects.add("nmss_multilive");
        this.allowedProjects.add("syslog");
        this.allowedProjects.add("whale_browser");
    }

    static synchronized void prepareElasticSearch(Map stormConf) throws UnknownHostException {
        if (client == null) {
            Map<String, Object> esConf = (Map<String, Object>) stormConf.get("elasticsearch");
            boolean tribeMode = esConf.containsKey("tribeMode")
                && Boolean.parseBoolean(esConf.get("tribeMode").toString());
            String clusterName = esConf.get("clusterName").toString();
            String[] ESHost = esConf.get("hosts").toString().split(",");
            int esPort = (int)Double.parseDouble(esConf.get("port").toString());
            Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .put("client.transport.sniff", tribeMode ? "false" : "true").build();
            List<InetSocketTransportAddress> transportAddressList = new ArrayList<>();
            for (String host : ESHost) {
                transportAddressList.add(new InetSocketTransportAddress(InetAddress.getByName(host), esPort));
            }
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(
                        transportAddressList.toArray(
                            new InetSocketTransportAddress[transportAddressList.size()]));
        }
    }

    void removeSystemFields(Map<String, Object> logMap) {
        if (logMap.containsKey(FIELD_ID)) {
            logMap.remove(FIELD_ID);
        }
        if (logMap.containsKey(FIELD_ROUTING)) {
            logMap.remove(FIELD_ROUTING);
        }
        if (logMap.containsKey(FIELD_LIMIT)) {
            logMap.remove(FIELD_LIMIT);
        }
    }
    String getIndex(Map<String, Object> messageMap) {
        Calendar cal = Calendar.getInstance();
        String project = (String) messageMap.get(Constants.FIELD_PROJECTNAME);

        Object obj = messageMap.get(FIELD_LOGTIME);
        if (obj instanceof Long) {
            cal.setTimeInMillis((Long) obj);
        } else if (obj instanceof String) {
            String logTime = (String) obj;
            try {
                cal.setTimeInMillis(Long.parseLong(logTime));
            } catch (NumberFormatException e) {
                logger.error(e.getMessage(), e);
            }
        }

        return indexPrefix + dateFormat.format(cal.getTime()) + "-" + project;
    }

    boolean isKnownFailure(String failureMessage) {
        if (failureMessage == null || knownFailureList == null) {
            return false;
        }

        for (String aKnownFailureList : knownFailureList) {
            if (failureMessage.contains(aKnownFailureList)) {
                return true;
            }
        }

        return false;
    }
    boolean isAllowedProject(String projectName) {
        if (StringUtils.isEmpty(projectName)) {
            return false;
        }
        return allowedProjects.contains(projectName);
    }
}
