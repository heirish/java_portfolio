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

    /**
     * input:
     *      tokens
     *      name, nodeLevel - where to find, find scope
     *      maxDistance - scope
     */
    public static String findCluster(String name, int nodeLevel, List<String> tokens, double maxDistance) {
        Map<String, PatternNode> clusters = PatternForest.getInstance().getNodes(name, nodeLevel);
        if (clusters != null) {
            for (Map.Entry<String, PatternNode> cluster : clusters.entrySet()) {
                if (belongsToCluster(tokens, cluster.getValue().getRepresentTokens(), maxDistance)) {
                    return cluster.getKey();
                }
            }
        }

        //TODO: Add new parent Node, first synchronize
        // 1. get new Node Id, if success, add new Node,
        // else synchronize new Nodes and recompute maxDistance, till get the cluster
        int triedTimes = 0;
        PatternNode parentNode= new PatternNode(tokens);
        do {
            String nodeId = PatternForest.getInstance().addNode(name, nodeLevel, parentNode);
            if (!StringUtils.isEmpty(nodeId)) {
                return nodeId;
            } else {
                Map<String, PatternNode> newLevelNodes = PatternForest.getInstance().getNodes(name, nodeLevel);
                MapDifference<String, PatternNode> diff = Maps.difference(clusters, newLevelNodes);
                for (Map.Entry<String, PatternNode> cluster : diff.entriesOnlyOnRight().entrySet()) {
                    if (belongsToCluster(tokens, cluster.getValue().getRepresentTokens(), maxDistance)) {
                        return cluster.getKey();
                    }
                    clusters.put(cluster.getKey(), cluster.getValue());
                }
                triedTimes++;
            }
        } while (triedTimes < Constants.FINDCLUSTER_TOLERANCE_TIMES);

        return "";
    }
}
