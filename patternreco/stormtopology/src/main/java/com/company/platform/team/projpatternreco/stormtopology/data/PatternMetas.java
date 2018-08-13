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
    private static int FIND_TOLERENCE_DEFAULT = 2;
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

    private PatternMetas(Map conf, RedisUtil redisUtil) {
        projectMetas = new ConcurrentHashMap<>();
        this.redisUtil = redisUtil;

        if (conf != null) {
            parseConfiguredGlobalMetas(conf);
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

    public static synchronized PatternMetas getInstance(Map conf, RedisUtil redisUtil) {
        if (instance == null) {
            instance = new PatternMetas(conf, redisUtil);
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

    public int getProjectId(String projectName) {
        int projectId = -1;
        try {
            synchronizeMetaFromRedis(projectName, PatternMetaType.PROJECT_ID);
            String metaKey = getMetaKey(projectName, PatternMetaType.PROJECT_ID);
            projectId = Integer.parseInt(projectMetas.get(metaKey));
        } catch (Exception e) {
            logger.error("get id for project " + projectName + " failed");
        }
        return projectId;
    }
    public int getProjectLeafCountMax(String projectName) {
        int leafCountMax;
        try {
            synchronizeMetaFromRedis(projectName, PatternMetaType.LEAF_NODES_LIMIT);
            String metaKey = getMetaKey(projectName, PatternMetaType.LEAF_NODES_LIMIT);
            leafCountMax = Integer.parseInt(projectMetas.get(metaKey));
            if (leafCountMax <= 0) {
                leafCountMax = LEAF_COUNT_MAX_DEFAULT;
                logger.info("invalid leafCountMax for project " + projectName + ", use defualt value " + leafCountMax);
            }
        } catch (Exception e) {
            leafCountMax = LEAF_COUNT_MAX_DEFAULT;
            logger.warn("get leafCountMax for project " + projectName + " failed. use default Value: " + leafCountMax);
        }
        return leafCountMax;
    }
    public double getProjectSimilarity(PatternLevelKey levelKey) {
        double leafSimilarity = getProjectLeafSimilarity(levelKey.getProjectName());
        double similarity =  leafSimilarity * Math.pow(1-similarityDecayFactor, levelKey.getLevel());
        double roundedSimilarity = CommonUtil.round(similarity, Constants.SIMILARITY_PRECISION);
        return roundedSimilarity;
    }
    public boolean stepUpProjectLeafSimilarity(String projectName) {
        double oldSimilarity = getProjectLeafSimilarity(projectName);
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
    public boolean stepDownProjectLeafSimilarity(String projectName) {
        double oldSimilarity = getProjectLeafSimilarity(projectName);
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
    private double getProjectLeafSimilarity(String projectName) {
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