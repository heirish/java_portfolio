package com.company.platform.team.projspark.data;

import com.company.platform.team.projspark.modules.FastClustering;
import com.company.platform.team.projspark.modules.PatternTreeHelper;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by admin on 2018/6/29.
 * Singletone
 */
// TODO:thread safe
public final class PatternLeaves {
    //Map<project, Map<project-level-nodeid, PatternNode>>
    private Map<String, Map<String, PatternNode>> patternNodes;
    private PatternTreeHelper treeHelper;
    private static PatternLeaves forest = new PatternLeaves();

    private PatternLeaves() {
        //TODO:recover from local checkpoint
        patternNodes = new HashMap<>();
        treeHelper = new PatternTreeHelper();
        Map<String, PatternNode> nodes = treeHelper.getAllLeaves();
        //TODO: split nodes by project
    }

    public static PatternLeaves getInstance() {
        return forest;
    }

    //TODO:input a distance function
    public String getParentNodeId(List<String> tokens, String projectName, double maxDistance) {
        Map<String, PatternNode> levelNodes = getNodes(projectName);
        if (levelNodes != null) {
            for (Map.Entry<String, PatternNode> node: levelNodes.entrySet()) {
                if (FastClustering.belongsToCluster(tokens,
                        node.getValue().getRepresentTokens(), maxDistance)) {
                    return node.getKey();
                }
            }
        }

        int triedTimes = 0;
        PatternNode parentNode= new PatternNode(tokens);
        do {
            String nodeId = addLeaf(projectName, parentNode);
            if (!StringUtils.isEmpty(nodeId)) {
                return nodeId;
            } else {
                Map<String, PatternNode> newLevelNodes = getNodes(projectName);
                MapDifference<String, PatternNode> diff = Maps.difference(levelNodes, newLevelNodes);
                for (Map.Entry<String, PatternNode> node : diff.entriesOnlyOnRight().entrySet()) {
                    if (FastClustering.belongsToCluster(tokens,
                            node.getValue().getRepresentTokens(), maxDistance)) {
                        return node.getKey();
                    }
                    levelNodes.put(node.getKey(), node.getValue());
                }
                triedTimes++;
            }
        } while (triedTimes < Constants.FINDCLUSTER_TOLERANCE_TIMES);

        return "";
    }


    public String addLeaf(String projectName, PatternNode node) {
        try {
            String nodeId = UUID.randomUUID().toString().replace("-", "");
            if (treeHelper.addNodesToCenter(projectName, 0, nodeId, node)) {
                String nodeKey = String.format("%s%s%s%s%s",
                        projectName, Constants.PATTERN_NODE_KEY_DELIMITER,
                        0, Constants.PATTERN_NODE_KEY_DELIMITER, nodeId);
                if (patternNodes.containsKey(projectName)) {
                    patternNodes.get(projectName).put(nodeKey, node);
                } else {
                    Map<String, PatternNode> nodes = new HashMap<>();
                    nodes.put(nodeKey, node);
                    patternNodes.put(projectName, nodes);
                }
                return nodeKey;
            }
        } catch (Exception e) {
           return "";
        }
        return "";
    }

    public Map<String, PatternNode> getNodes(String projectName) {
        //For java pass object by reference
        Map<String, PatternNode> nodes = new HashMap<>();
        if (patternNodes.containsKey(projectName)) {
            nodes.putAll(patternNodes.get(projectName));
        }
        return nodes;
    }

    public String visualize(String name) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(String.format("projectName: %s:", ""));
        stringBuilder.append(System.getProperty("line.separator"));
        return "";
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, Map<String, PatternNode>> entry : patternNodes.entrySet()) {
            stringBuilder.append("project " + entry.getKey() + " leaves:");
            stringBuilder.append(System.getProperty("line.separator"));
            for (Map.Entry<String, PatternNode> entryNode : entry.getValue().entrySet()) {
                stringBuilder.append(String.format("key: %s\tvalue:%s",
                        entryNode.getKey(), entryNode.getValue().toString()));
                stringBuilder.append(System.getProperty("line.separator"));
            }
        }
        return stringBuilder.toString();
    }

    public void saveToFile(String fileName) {
        try {
            FileWriter fw = new FileWriter(fileName);
            fw.write(PatternLeaves.getInstance().toString());
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
