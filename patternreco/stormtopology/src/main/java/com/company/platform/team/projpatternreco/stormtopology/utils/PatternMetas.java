package com.company.platform.team.projpatternreco.stormtopology.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by admin on 2018/7/27.
 */
public class PatternMetas {
    private static Logger logger = LoggerFactory.getLogger(PatternMetas.class);

    public static final double LEAF_SIMILARITY_MIN_DEFAULT = 0.1;
    public static final double LEAF_SIMILARITY_MAX_DEFAULT = 0.9;
    public static final double SIMILARITY_DECAY_FACTOR_DEFAULT = 0.1;
    public static final int LEAF_COUNT_MAX_DEFAULT = 500;
    public static final int FINDCLUSTER_TOLERANCE_TIMES = 4;
    public static final int BODY_LENGTH_MAX_DEFAULT = 5000;
    public static final int TOKEN_COUNT_MAX_DEFAULT = 200;
    public static final int PATTERN_LEVEL_MAX_DEFAULT = 10;

    private ConcurrentHashMap<String, String> projectMetas;

    private static PatternMetas instance;

    private PatternMetas(Map conf) {
        //TODO: load meta from somewhere
    }

    public static synchronized PatternMetas getInstance(Map conf) {
        if (instance == null) {
            instance = new PatternMetas(conf);
        }
        return instance;
    }

    public double getLeafSimilarityMin(String projectName) {
        double similarityMin;
        try {
            String metaKey = getMetaKey(projectName, PatternMetaType.LEAF_SIMILARITY_MIN);
            similarityMin = Double.parseDouble(projectMetas.get(metaKey));
        } catch (Exception e) {
            similarityMin = LEAF_SIMILARITY_MIN_DEFAULT;
            logger.warn("get similarity min for project: " + projectName
                    + "failed, use default value: " + similarityMin);
        }
        return similarityMin;
    }

    public double getLeafSimilarityMax(String projectName) {
        double similarityMax;
        try {
            String metaKey = getMetaKey(projectName, PatternMetaType.LEAF_SIMILARITY_MAX);
            similarityMax = Double.parseDouble(projectMetas.get(metaKey));
        } catch (Exception e) {
            similarityMax = LEAF_SIMILARITY_MAX_DEFAULT;
            logger.warn("get similarity max for project: " + projectName
                    + "failed, use default value: " + similarityMax);
        }
        return similarityMax;

    }

    public double getSimilarityDecayFactor(String projectName) {
        double decayFactor;
        try {
            String metaKey = getMetaKey(projectName, PatternMetaType.DECAY_FACTOR);
            decayFactor = Double.parseDouble(projectMetas.get(metaKey));
        } catch (Exception e) {
            decayFactor = SIMILARITY_DECAY_FACTOR_DEFAULT;
            logger.warn("get similarity decay factor for project: " + projectName
                    + "failed, use default value: " + decayFactor);
        }
        return decayFactor;

    }

    public int getFindTolerance(String projectName) {
        int findTolerance;
        try {
            String metaKey = getMetaKey(projectName, PatternMetaType.FIND_TOLERANCE);
            findTolerance = Integer.parseInt(projectMetas.get(metaKey));
        } catch (Exception e) {
            findTolerance = FINDCLUSTER_TOLERANCE_TIMES;
            logger.warn("get find cluster tolerance for project: " + projectName
                    + "failed, use default value: " + findTolerance);
        }
        return findTolerance;

    }

    public int getLeafNodesLimit(String projectName) {
        int limit;
        try {
            String metaKey = getMetaKey(projectName, PatternMetaType.LEAF_NODES_LIMIT);
            limit = Integer.parseInt(projectMetas.get(metaKey));
        } catch (Exception e) {
            limit = LEAF_COUNT_MAX_DEFAULT;
            logger.warn("get leaf nodes limit for project: " + projectName
                    + "failed, use default: " + limit);
        }
        return limit;
    }

    public int getPatternLevelMax(String projectName) {
        int levelMax;
        try {
            String metaKey = getMetaKey(projectName, PatternMetaType.PATTERN_LEVEL_MAX);
            levelMax = Integer.parseInt(projectMetas.get(metaKey));
        } catch (Exception e) {
            levelMax = PATTERN_LEVEL_MAX_DEFAULT;
            logger.warn("get pattern level limit for project: " + projectName
                    + "failed, use default: " + levelMax);
        }
        return levelMax;
    }

    public int getBodyLengthMax(String projectName) {
        int bodyLengthMax;
        try {
            String metaKey = getMetaKey(projectName, PatternMetaType.BODY_LENGTH_MAX);
            bodyLengthMax = Integer.parseInt(projectMetas.get(metaKey));
        } catch (Exception e) {
            bodyLengthMax = BODY_LENGTH_MAX_DEFAULT;
            logger.warn("get body length limit for project: " + projectName
                    + "failed, use default: " + bodyLengthMax);
        }
        return bodyLengthMax;
    }

    public int getTokensCountMax(String projectName) {
        int tokensCountMax;
        try {
            String metaKey = getMetaKey(projectName, PatternMetaType.BODY_LENGTH_MAX);
            tokensCountMax = Integer.parseInt(projectMetas.get(metaKey));
        } catch (Exception e) {
            tokensCountMax = TOKEN_COUNT_MAX_DEFAULT;
            logger.warn("get token count limit for project: " + projectName
                    + "failed, use default: " + tokensCountMax);
        }
        return tokensCountMax;
    }

    private String getMetaKey(String projectName, PatternMetaType type) {
        String metaKey = String.format("%s%s%s",
                projectName, ":", type.getTypeString());
        return metaKey;
    }
}