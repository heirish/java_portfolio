package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.data.*;
import com.company.platform.team.projspark.preprocess.Identifier;
import com.company.platform.team.projspark.preprocess.Tokenizer;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

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
