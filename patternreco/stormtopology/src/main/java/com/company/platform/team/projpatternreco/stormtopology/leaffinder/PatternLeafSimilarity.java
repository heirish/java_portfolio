package com.company.platform.team.projpatternreco.stormtopology.leaffinder;

import com.company.platform.team.projpatternreco.stormtopology.utils.Constants;
import com.company.platform.team.projpatternreco.stormtopology.utils.PatternRecognizeException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by admin on 2018/7/27.
 */
public class PatternLeafSimilarity {
    private ConcurrentHashMap<String, Double> projectSimilarities;
    private double similarityDefault;

    private static PatternLeafSimilarity patternLeafSimilarity;

    private PatternLeafSimilarity(double similarity) {
        projectSimilarities = new ConcurrentHashMap<>();
        if (similarity < 0.1 || similarity > 0.99) {
            similarityDefault = Constants.PATTERN_LEAF_SIMILARITY_DEFAULT;
        } else {
            similarityDefault = similarity;
        }
    }

    public static synchronized PatternLeafSimilarity getInstance(double similarity) {
        if (patternLeafSimilarity == null) {
            patternLeafSimilarity = new PatternLeafSimilarity(similarity);
        }
        return patternLeafSimilarity;
    }

    public double getSimilarity(String projectName) {
        if (!projectSimilarities.containsKey(projectName)) {
            projectSimilarities.put(projectName, similarityDefault);
        }
        return projectSimilarities.get(projectName);
    }

    public synchronized double decaySimilarity(String projectName) {
        projectSimilarities.put(projectName, getSimilarity(projectName) - Constants.PATTERN_SIMILARITY_STEPWISE);
        return projectSimilarities.get(projectName);
    }
}
