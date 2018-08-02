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

    private ConcurrentHashMap<String, String> projectMetas;
    private RedisNodeCenter nodeCenter;

    private static PatternMetas instance;

    private PatternMetas(Map conf) {
        nodeCenter = RedisNodeCenter.getInstance(conf);
    }

    public static synchronized PatternMetas getInstance(Map conf) {
        if (instance == null) {
            instance = new PatternMetas(conf);
        }
        return instance;
    }

    public double getLeafSimilarity(String projectName) {
        double similarity;
        try {
            String leafSimilarity = nodeCenter.getMeta(projectName, PatternMetaType.LEAF_SIMILARITY.toString());
            similarity = Double.parseDouble(leafSimilarity);
        } catch (Exception e) {
            similarity = Constants.PATTERN_LEAF_SIMILARITY_DEFAULT;
            logger.info("project " + projectName + " has no or invalid leaf similarity meta data, will use default: " + similarity);
        }
        return similarity;
    }

    public void setLeafSimilarity(String projectName, double similarity) {
        nodeCenter.setMeta(projectName, PatternMetaType.LEAF_SIMILARITY.toString(), String.valueOf(similarity));
        //TODO: redis publisher
    }

    public double getLeafSimilarityMax(String projectName) {
        double similarity;
        try {
            String leafSimilarity = nodeCenter.getMeta(projectName, PatternMetaType.LEAF_SIMILARITY_MAX.toString());
            similarity = Double.parseDouble(leafSimilarity);
        } catch (Exception e) {
            similarity = Constants.PATTERN_LEAF_SIMILARITY_MAX;
            logger.info("project " + projectName + " has no or invalid leaf similarity max meta data, will use default: " + similarity);
        }
        return similarity;
    }

    public double getLeafSimilarityMin(String projectName) {
        double similarity;
        try {
            String leafSimilarity = nodeCenter.getMeta(projectName, PatternMetaType.LEAF_SIMILARITY_MIN.toString());
            similarity = Double.parseDouble(leafSimilarity);
        } catch (Exception e) {
            similarity = Constants.PATTERN_LEAF_SIMILARITY_MIN;
            logger.info("project " + projectName + " has no or invalid leaf similarity min meta data, will use default: " + similarity);
        }
        return similarity;
    }

    public double stepUpLeafSimilarity(String projectName) {
        double oldSimilarity = getLeafSimilarity(projectName);
        double similarityMax = getLeafSimilarityMax(projectName);
        double newSimilarity = (oldSimilarity + similarityMax) / 2;
        setLeafSimilarity(projectName, newSimilarity);
        return newSimilarity;
    }

    public double stepDownLeafSimilarity(String projectName) {
        double oldSimilarity = getLeafSimilarity(projectName);
        double similarityMin = getLeafSimilarityMin(projectName);
        double newSimilarity = (oldSimilarity + similarityMin) / 2;
        setLeafSimilarity(projectName, newSimilarity);
        return newSimilarity;
    }

    public int getLeafNodesLimit(String projectName) {
        int limit;
        try {
            String leafCountLimit = nodeCenter.getMeta(projectName, PatternMetaType.LEAF_NODES_LIMIT.toString());
            limit = Integer.parseInt(leafCountLimit);
        } catch (Exception e) {
            limit = Constants.PATTERN_LEAF_COUNT_MAX_DEFAULT;
            logger.info("project " + projectName + " has no or invalid leaf Nodes limit meta data, will use default: " + limit);
        }
        return limit;
    }

    public int getPatternLevelMax(String projectName) {
        int levelMax;
        try {
            String patternLevelMax = nodeCenter.getMeta(projectName, PatternMetaType.PATTERN_LEVEL_MAX.toString());
            levelMax = Integer.parseInt(patternLevelMax);
        } catch (Exception e) {
            levelMax = Constants.PATTERN_LEVEL_MAX_DEFAULT;
            logger.info("project " + projectName + " has no or invalid pattern level max meta data, will use default: " + levelMax);
        }
        return levelMax;
    }

    public int getBodyLengthMax(String projectName) {
        int bodyLengthMax;
        try {
            String result = nodeCenter.getMeta(projectName, PatternMetaType.BODY_LENGTH_MAX.toString());
            bodyLengthMax = Integer.parseInt(result);
        } catch (Exception e) {
            bodyLengthMax = Constants.BODY_LENGTH_MAX_DEFAULT;
            logger.info("project " + projectName + " has no or invalid body length max meta data, will use default: " + bodyLengthMax);
        }
        return bodyLengthMax;
    }

    public int getTokensCountMax(String projectName) {
        int tokensCountMax;
        try {
            String result = nodeCenter.getMeta(projectName, PatternMetaType.TOKEN_COUNT_MAX.toString());
            tokensCountMax = Integer.parseInt(result);
        } catch (Exception e) {
            tokensCountMax = Constants.TOKEN_COUNT_MAX_DEFAULT;
            logger.info("project " + projectName + " has no or invalid tokens count max meta data, will use default: " + tokensCountMax);
        }
        return tokensCountMax;
    }
}