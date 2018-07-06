package com.company.platform.team.projspark.modules;

import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Created by admin on 2018/6/21.
 */
public class FastClustering {

    public static Boolean belongsToCluster(List<String> logTokens, List<String> representTokens, double maxDistance) {
        int minSize = Math.min(logTokens.size(), representTokens.size());
        int maxSize = Math.max(logTokens.size(), representTokens.size());
        if (maxSize == 0) {
            return true;
        }

        double minScore = (1-maxDistance) * maxSize;
        double score = 0;
        for (int i=0; i<minSize; i++) {
            if (StringUtils.endsWithIgnoreCase(logTokens.get(i), representTokens.get(i))) {
                score += 1;
            }
            if (score > minScore) {
                return true;
            }
        }

        return score > minScore ? true: false;
    }
}
