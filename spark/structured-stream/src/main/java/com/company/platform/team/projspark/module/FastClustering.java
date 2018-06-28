package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.data.PatternNode;
import com.company.platform.team.projspark.data.PatternTree;
import com.company.platform.team.projspark.preprocess.Identifier;
import com.company.platform.team.projspark.preprocess.Tokenizer;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/21.
 */
public class FastClustering {
    private static final Logger logger = Logger.getLogger("");
    private static Map<String, PatternTree> patternTrees = new HashMap<>();

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

    public static String findCluster(String projectName, String text, int nodeLevel, double maxDistance){
        if (!patternTrees.containsKey(projectName)) {
            patternTrees.put(projectName, new PatternTree(projectName));
        }
        PatternTree projectPatternTree = patternTrees.get(projectName);

        String preprocessedText = Identifier.identifyIP(text, "NELO_IP");
        preprocessedText = Identifier.identifyDatetime(preprocessedText, "NELO_DATETIME");

        List<String> tokens = Tokenizer.simpleTokenize(preprocessedText);
        Map<String, PatternNode> levelNodes = projectPatternTree.getNodes(nodeLevel);
        if (levelNodes != null) {
            for (Map.Entry<String, PatternNode> entry : levelNodes.entrySet()) {
                if (belongsToCluster(tokens, entry.getValue().getRepresentTokens(), maxDistance)) {
                    return entry.getValue().getNodeId();
                }
            }
        }

        //TODO: Add new Node, first synchronize
        // 1. get new Node Id, if success, add new Node,
        // else synchronize new Nodes and recompute maxDistance, till get the cluster
        int triedTimes = 0;
        while (StringUtils.isEmpty(patternTrees.get(projectName).addNode(nodeLevel, tokens))
                && triedTimes < Constants.FINDCLUSTER_TOLERANCE_TIMES) {
            Map<String, PatternNode> newLevelNodes = projectPatternTree.getNodes(nodeLevel);
            MapDifference<String, PatternNode> diff = Maps.difference(levelNodes, newLevelNodes);
            for (String nodeId: diff.entriesOnlyOnRight().keySet()) {
                if (belongsToCluster(tokens, newLevelNodes.get(nodeId).getRepresentTokens(), maxDistance)) {
                    return nodeId;
                }
            }
            triedTimes ++;
        }

        return "";
    }

    public static String getPatternTreeString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, PatternTree> entry : patternTrees.entrySet()) {
            stringBuilder.append(String.format("projectName: %s:", entry.getKey()));
            stringBuilder.append(System.getProperty("line.separator"));
            stringBuilder.append(String.format("\t%s", entry.getValue().toString()));
            stringBuilder.append(System.getProperty("line.separator"));
        }
        return stringBuilder.toString();
    }
}
