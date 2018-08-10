package com.company.platform.team.projpatternreco.stormtopology.data;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.stormtopology.eventbus.EventBus;
import com.company.platform.team.projpatternreco.stormtopology.eventbus.SimilarityEvent;
import com.company.platform.team.projpatternreco.stormtopology.utils.CommonUtil;
import com.company.platform.team.projpatternreco.stormtopology.utils.RedisUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by admin on 2018/7/27.
 */
public class PatternMetas {
    private static Logger logger = LoggerFactory.getLogger(PatternMetas.class);
    private static EventBus eventBusInstance = EventBus.getInstance();

    //hot configurable datas
    private static final int LEAF_COUNT_MAX_DEFAULT = 500;
    private ConcurrentHashMap<String, String> projectMetas;

    //configurable global metas
    private static double SIMILARITY_DECAY_FACTOR_DEFAULT = 0.1;
    private static double LEAF_SIMILARITY_MIN_DEFAULT = 0.5;
    private static double LEAF_SIMILARITY_MAX_DEFAULT = 0.9;
    private static int FIND_TOLERENCE_DEFAULT = 3;
    private static int PATTERN_LEVEL_MAX_DEFAULT = 10;
    private static int BODY_LENGTH_MAX_DEFAULT = 5000;
    private static int TOKEN_COUNT_MAX_DEFAULT = 200;
    private double similarityDecayFactor;
    private double leafSimilarityMin;
    private double leafSimilarityMax;
    private int findTolerence;
    private int patternLevelMax;
    private int bodyLengthMax;
    private int tokenCountMax;

    private RedisUtil redisUtil;

    private static PatternMetas instance;

    private PatternMetas(Map conf) {
        projectMetas = new ConcurrentHashMap<>();

        if (conf != null) {
            Map redisConf = (Map) conf.get(Constants.CONFIGURE_REDIS_SECTION);
            redisUtil = RedisUtil.getInstance(redisConf);

            Map patternrecoConf = (Map) conf.get(Constants.CONFIGURE_PATTERNRECO_SECTION);
            parseConfiguredGlobalMetas(patternrecoConf);
        } else {
            similarityDecayFactor = SIMILARITY_DECAY_FACTOR_DEFAULT;
            leafSimilarityMin = LEAF_SIMILARITY_MIN_DEFAULT;
            leafSimilarityMax = LEAF_SIMILARITY_MAX_DEFAULT;
            findTolerence = FIND_TOLERENCE_DEFAULT;
            patternLevelMax = PATTERN_LEVEL_MAX_DEFAULT;
            bodyLengthMax = BODY_LENGTH_MAX_DEFAULT;
            tokenCountMax = TOKEN_COUNT_MAX_DEFAULT;
        }
    }

    public static synchronized PatternMetas getInstance(Map conf) {
        if (instance == null) {
            instance = new PatternMetas(conf);
        }
        return instance;
    }

    public int getPatternLevelMax() {
        return patternLevelMax;
    }
    public int getBodyLengthMax() {
        return bodyLengthMax;
    }
    public int getTokenCountMax() {
        return tokenCountMax;
    }
    public int getFindTolerence() {
        return findTolerence;
    }
    public int getLeafCountMax(String projectName) {
        int leafCountMax;
        try {
            synchronizeMetaFromRedis(projectName, PatternMetaType.LEAF_NODES_LIMIT);
            String metaKey = getMetaKey(projectName, PatternMetaType.LEAF_NODES_LIMIT);
            leafCountMax = Integer.parseInt(projectMetas.get(metaKey));
        } catch (Exception e) {
            leafCountMax = LEAF_COUNT_MAX_DEFAULT;
            logger.warn("get leafCountMax for project " + projectName + " failed. use default Value: " + leafCountMax);
        }
        return leafCountMax;
    }

    public double getSimilarity(PatternLevelKey levelKey) {
        double leafSimilarity = getLeafSimilarity(levelKey.getProjectName());
        double similarity =  leafSimilarity * Math.pow(1-similarityDecayFactor, levelKey.getLevel());
        double roundedSimilarity = CommonUtil.round(similarity, Constants.SIMILARITY_PRECISION);
        return roundedSimilarity;
    }
    public boolean stepUpLeafSimilarity(String projectName) {
        double oldSimilarity = getLeafSimilarity(projectName);
        double newSimilarity =  CommonUtil.round((oldSimilarity + leafSimilarityMax) / 2, Constants.SIMILARITY_PRECISION);
        if (CommonUtil.equalWithPrecision(oldSimilarity, newSimilarity, Constants.SIMILARITY_PRECISION)) {
            return false;
        }
        String metaKey = getMetaKey(projectName, PatternMetaType.LEAF_SIMILARITY);
        projectMetas.put(metaKey, String.valueOf(newSimilarity));
        setMetaToRedis(projectName, PatternMetaType.LEAF_SIMILARITY);

        publishSimilarityEvent(projectName);
        return true;
    }
    public boolean stepDownLeafSimilarity(String projectName) {
        double oldSimilarity = getLeafSimilarity(projectName);
        double newSimilarity = CommonUtil.round((oldSimilarity + leafSimilarityMin) / 2, Constants.SIMILARITY_PRECISION);
        if (CommonUtil.equalWithPrecision(oldSimilarity, newSimilarity, Constants.SIMILARITY_PRECISION)) {
            return false;
        }
        String metaKey = getMetaKey(projectName, PatternMetaType.LEAF_SIMILARITY);
        projectMetas.put(metaKey, String.valueOf(newSimilarity));
        setMetaToRedis(projectName, PatternMetaType.LEAF_SIMILARITY);

        publishSimilarityEvent(projectName);
        return true;
    }
    private double getLeafSimilarity(String projectName) {
        double leafSimilarity;
        try {
            synchronizeMetaFromRedis(projectName, PatternMetaType.LEAF_SIMILARITY);
            String metaKey = getMetaKey(projectName, PatternMetaType.LEAF_SIMILARITY);
            leafSimilarity = Double.parseDouble(projectMetas.get(metaKey));
        } catch (Exception e) {
            leafSimilarity = leafSimilarityMax;
            logger.debug("get similarity for project " + projectName + " failed, use default: " + leafSimilarity);
        }
        return leafSimilarity;
    }

    public boolean isPatternNew(String projectName) {
        boolean isNew;
        try {
            synchronizeMetaFromRedis(projectName, PatternMetaType.PATTERN_IS_NEW);
            String metaKey = getMetaKey(projectName, PatternMetaType.PATTERN_IS_NEW);
            isNew = Boolean.parseBoolean(projectMetas.get(metaKey));
        } catch (Exception e) {
            isNew = true;
            logger.debug("get pattern new status for project " + projectName + " failed, use default: " + isNew);
        }
        return isNew;
    }
    public void setPatternNew(String projectName, boolean status) {
        String metaKey = getMetaKey(projectName, PatternMetaType.PATTERN_IS_NEW);
        projectMetas.put(metaKey, String.valueOf(status));
        setMetaToRedis(projectName, PatternMetaType.PATTERN_IS_NEW);
    }

    private void synchronizeMetaFromRedis(String projectName, PatternMetaType type) {
        String metaKey = getMetaKey(projectName, type);
        String oldValue = projectMetas.get(metaKey);

        String redisValue = redisUtil.getMetaData(projectName, type.getTypeString());
        if (type == PatternMetaType.LEAF_SIMILARITY
            && !StringUtils.equals(oldValue, redisValue)) {
            publishSimilarityEvent(projectName);
        } else {
            ;
        }
        //set local Value;
        projectMetas.put(metaKey, redisValue);
    }
    private void setMetaToRedis(String projectName, PatternMetaType type) {
        String metaKey = getMetaKey(projectName, type);
        String value = projectMetas.get(metaKey);
        redisUtil.setMetaData(projectName, type.getTypeString(), value);
    }

    private void publishSimilarityEvent(String projectName) {
        SimilarityEvent event = new SimilarityEvent();
        event.setProjectName(projectName);
        eventBusInstance.publish(event);
    }

    private String getMetaKey(String projectName, PatternMetaType type) {
        String metaKey = String.format("%s%s%s",
                projectName, ":", type.getTypeString());
        return metaKey;
    }

    private void parseConfiguredGlobalMetas(Map conf) {
        String metaTypeString = PatternMetaType.DECAY_FACTOR.getTypeString();
        try {
            similarityDecayFactor = Double.parseDouble(conf.get(metaTypeString).toString());
        } catch (Exception e) {
            similarityDecayFactor = SIMILARITY_DECAY_FACTOR_DEFAULT;
            logger.warn("get " + metaTypeString + " from config failed, use default: " + similarityDecayFactor);
        }

        metaTypeString = PatternMetaType.LEAF_SIMILARITY_MIN.getTypeString();
        try {
            leafSimilarityMin= Double.parseDouble(conf.get(metaTypeString).toString());
        } catch (Exception e) {
            leafSimilarityMin = LEAF_SIMILARITY_MIN_DEFAULT;
            logger.warn("get " + metaTypeString + " from config failed, use default: " + leafSimilarityMin);
        }

        metaTypeString = PatternMetaType.LEAF_SIMILARITY_MAX.getTypeString();
        try {
            leafSimilarityMax= Double.parseDouble(conf.get(metaTypeString).toString());
        } catch (Exception e) {
            leafSimilarityMax = LEAF_SIMILARITY_MAX_DEFAULT;
            logger.warn("get " + metaTypeString + " from config failed, use default: " + leafSimilarityMax);
        }

        metaTypeString = PatternMetaType.FIND_TOLERANCE.getTypeString();
        try {
            findTolerence = Integer.parseInt(conf.get(metaTypeString).toString());
        } catch (Exception e) {
            findTolerence = FIND_TOLERENCE_DEFAULT;
            logger.warn("get " + metaTypeString + " from config failed, use default: " + findTolerence);
        }

        metaTypeString = PatternMetaType.PATTERN_LEVEL_MAX.getTypeString();
        try {
            patternLevelMax = Integer.parseInt(conf.get(metaTypeString).toString());
        } catch (Exception e) {
            patternLevelMax = PATTERN_LEVEL_MAX_DEFAULT;
            logger.warn("get " + metaTypeString + " from config failed, use default: " + patternLevelMax);
        }

        metaTypeString = PatternMetaType.BODY_LENGTH_MAX.getTypeString();
        try{
            bodyLengthMax = Integer.parseInt(conf.get(metaTypeString).toString());
        } catch (Exception e) {
            bodyLengthMax = BODY_LENGTH_MAX_DEFAULT;
            logger.warn("get " + metaTypeString + " from config failed, use default: " + bodyLengthMax);
        }

        metaTypeString = PatternMetaType.TOKEN_COUNT_MAX.getTypeString();
        try {
            tokenCountMax = Integer.parseInt(conf.get(metaTypeString).toString());
        } catch (Exception e) {
            tokenCountMax = TOKEN_COUNT_MAX_DEFAULT;
            logger.warn("get " + metaTypeString + " from config failed, use default: " + tokenCountMax);
        }
    }
}