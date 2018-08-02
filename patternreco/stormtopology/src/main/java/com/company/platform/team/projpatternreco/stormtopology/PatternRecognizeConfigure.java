package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.stormtopology.utils.RunningType;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.apache.storm.Config;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;

/**
 * Created by admin on 2018/7/12.
 */
public class PatternRecognizeConfigure {
    private static final Logger logger = LoggerFactory.getLogger("");
    private static final Gson gson = new Gson();
    private final Config stormConf;
    private static PatternRecognizeConfigure instance;
    private static final int DEFAULT_WORKERS = 4;
    private static final int DEFAULT_FETCH_SIZE = 50 * 1024 * 1024;

    private PatternRecognizeConfigure(String fileName, RunningType runningType) {
        stormConf = new Config();
        Map confMap = null;
        try {
            if (runningType == RunningType.LOCAL) {
                confMap = gson.fromJson(new InputStreamReader(
                                Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream(fileName))),
                        Map.class);
            } else {
                confMap = gson.fromJson(new JsonReader(new FileReader(fileName)), Map.class);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // confMap have to not null
        // or it make NPE occur
        assert confMap != null;

        logger.info("[stormConf] stormConf : " + confMap);
        //noinspection unchecked
        stormConf.putAll(confMap);
        stormConf.setDebug(false);
        stormConf.setNumWorkers(getParallelismCount("worker"));
        stormConf.setNumAckers(getParallelismCount("acker"));
        stormConf.setMaxTaskParallelism(getIntegerValueFromMap("topology", "maxParallelism", 1024));
        stormConf.setMaxSpoutPending(getIntegerValueFromMap("topology", "maxSpoutPending", 1024));
        stormConf.put(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS, (getIntegerValueFromMap("topology", "sleepSpoutWaitMs", 100)));
        stormConf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, (getDoubleValueFromMap("topology", "sampleRate", 0.051)));
        stormConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, (getIntegerValueFromMap("topology", "messageTimeoutSec", 1)));
    }

    public static synchronized PatternRecognizeConfigure getInstance(String fileName, RunningType runningType) {
        if (instance == null) {
            instance = new PatternRecognizeConfigure(fileName, runningType);
        }
        return instance;
    }

    public Map getConfigMap(String topics) {
        return (Map) stormConf.get(topics);
    }

    private String getStringValueFromMap(String key, String subKey, String defaultValue) {
        try {
            return (getConfigMap(key)).get(subKey).toString();
        } catch (Exception e) {
            logger.error("[ERROR] get String value from config file, so using default value > key : " + key + " / subKey : " + subKey + " / default value : " + defaultValue);
            return defaultValue;
        }
    }

    private String getStringValueFromMap(String key, String subKey) throws RuntimeException {
        try {
            return (getConfigMap(key)).get(subKey).toString();
        } catch (Exception e) {
            logger.error("[ERROR] get String value from config file, so throw runtime Exception > key : " + key + " / subKey : " + subKey);
            throw new RuntimeException("[Excpetion] Can not get value from map > key : " + key + " / subKey : " + subKey);
        }
    }

    private Integer getIntegerValueFromMap(String key, String subKey, Integer defaultValue) {
        try {
            return getDoubleValueFromMap(key, subKey, defaultValue.doubleValue()).intValue();
        } catch (Exception e) {
            logger.error("[ERROR] get Integer value from config file, so using default value > key : " + key + " / subKey : " + subKey + " / default value : " + defaultValue);
            return defaultValue;
        }
    }

    private Double getDoubleValueFromMap(String key, String subKey, Double defaultValue) {
        try {
            return (Double) (getConfigMap(key)).get(subKey);
        } catch (Exception e) {
            logger.error("[ERROR] get Double value from config file, so using default value > key : " + key + " / subKey : " + subKey + " / default value : " + defaultValue);
            return defaultValue;
        }
    }

    // for worker/acker
    public Integer getParallelismCount(String parallelismBolt) {
        return getIntegerValueFromMap("parallelismCount", parallelismBolt, DEFAULT_WORKERS);
    }

    public String getKafkaBrokerHosts() {
        return getStringValueFromMap("kafka", "brokers");
    }

    public KafkaSpoutConfig.FirstPollOffsetStrategy getOffsetStrategy() {
        Map kafkaConf = getConfigMap("kafka");

        if (kafkaConf.containsKey("consumeFromLatest") && (boolean) kafkaConf.get("consumeFromLatest")) {
            return KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;
        } else if (kafkaConf.containsKey("consumeFromEarliest") && (boolean) kafkaConf.get("consumeFromEarliest")) {
            return KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
        } else {
            return KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;
        }
    }

    public String getProducerSerializer() {
        return "org.apache.kafka.common.serialization.StringSerializer";
    }

    public int getFetchSizeBytes() {
        try {
            return getIntegerValueFromMap("kafka", "fetchMega", 50) * 1024 * 1024;
        } catch (Exception e) {
            return DEFAULT_FETCH_SIZE;
        }
    }

    Config getStormConfig() {
        return stormConf;
    }
}
