package com.company.platform.team.projspark.data;

import com.company.platform.team.projspark.modules.FastClustering;
import com.company.platform.team.projspark.modules.PatternTreeHelper;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by admin on 2018/6/29.
 * Singletone
 */
// TODO:thread safe
public final class PatternLevelTree {
    //Map<project-level, Map<project-level-nodeid, PatternNode>>
    private Map<String, Map<String, PatternNode>> patternNodes;
    private PatternTreeHelper treeHelper;
    private static PatternLevelTree forest = new PatternLevelTree();

    private PatternLevelTree() {
        //TODO:recover from local checkpoint
        patternNodes = new HashMap<>();
        treeHelper = new PatternTreeHelper();
        Map<String, PatternNode> nodes = treeHelper.getAllNodes();
        //TODO: split nodes by project+level
    }

    public static PatternLevelTree getInstance() {
        return forest;
    }

    //TODO:input a distance function
    public String getParentNodeId(List<String> tokens, String projectName, int nodeLevel, double maxDistance) {
        Map<String, PatternNode> levelNodes = getNodes(projectName, String.format("%s", nodeLevel));
        if (levelNodes != null) {
            for (Map.Entry<String, PatternNode> node: levelNodes.entrySet()) {
                if (FastClustering.belongsToCluster(tokens, node.getValue().getRepresentTokens(), maxDistance)) {
                    return node.getKey();
                }
            }
        }

        int triedTimes = 0;
        PatternNode parentNode= new PatternNode(tokens);
        do {
            String nodeId = addNode(projectName, nodeLevel, parentNode);
            if (!StringUtils.isEmpty(nodeId)) {
                return nodeId;
            } else {
                Map<String, PatternNode> newLevelNodes = getNodes(projectName, String.format("%s", nodeLevel));
                MapDifference<String, PatternNode> diff = Maps.difference(levelNodes, newLevelNodes);
                for (Map.Entry<String, PatternNode> node : diff.entriesOnlyOnRight().entrySet()) {
                    if (FastClustering.belongsToCluster(tokens, node.getValue().getRepresentTokens(), maxDistance)) {
                        return node.getKey();
                    }
                    levelNodes.put(node.getKey(), node.getValue());
                }
                triedTimes++;
            }
        } while (triedTimes < Constants.FINDCLUSTER_TOLERANCE_TIMES);

        return "";
    }

    public void updateNode(String projectName, int nodeLevel, String nodeId, PatternNode node) throws Exception{
        String levelKey = String.format("%s%s%s", projectName, Constants.PATTERN_NODE_KEY_DELIMITER, nodeLevel);
        String nodeKey = String.format("%s%s%s", levelKey, Constants.PATTERN_NODE_KEY_DELIMITER, nodeId);
        if (patternNodes.containsKey(levelKey) && patternNodes.get(levelKey).containsKey(nodeKey)) {
            if(treeHelper.updateNodesToCenter(projectName, nodeLevel, nodeId, node)) {
                patternNodes.get(levelKey).put(nodeKey, node);
            } else {
                throw new Exception("udpate to Nodes center failed");
            }
        } else {
            throw new Exception("node not exists");
        }
    }

    public String addNode(String projectName, int nodeLevel, PatternNode node) {
        try {
            String nodeId = UUID.randomUUID().toString().replace("-", "");
            if (treeHelper.addNodesToCenter(projectName, nodeLevel, nodeId, node)) {
                String levelKey = String.format("%s%s%s", projectName, Constants.PATTERN_NODE_KEY_DELIMITER, nodeLevel);
                String nodeKey = String.format("%s%s%s", levelKey, Constants.PATTERN_NODE_KEY_DELIMITER, nodeId);
                if (patternNodes.containsKey(levelKey)) {
                    patternNodes.get(levelKey).put(nodeKey, node);
                } else {
                    Map<String, PatternNode> nodes = new HashMap<>();
                    nodes.put(nodeKey, node);
                    patternNodes.put(levelKey, nodes);
                }
                return nodeKey;
            }
        } catch (Exception e) {
           return "";
        }
        return "";
    }

    public PatternNode getNode(String nodeKey) {
        String projectName = nodeKey.split(Constants.PATTERN_NODE_KEY_DELIMITER)[0];
        String nodeLevel = nodeKey.split(Constants.PATTERN_NODE_KEY_DELIMITER)[1];
        return new PatternNode(getNodes(projectName, nodeLevel).get(nodeKey));
    }

    public PatternNode getNode(String projectName, int nodeLevel, String nodeId) {
        String nodeKey = String.format("%s%s%s%s%s",
                projectName, Constants.PATTERN_NODE_KEY_DELIMITER,
                nodeLevel, Constants.PATTERN_NODE_KEY_DELIMITER,
                nodeId);
        return new PatternNode(getNodes(projectName, String.format("%s", nodeLevel)).get(nodeKey));
    }

    public Map<String, PatternNode> getNodes(String projectName, String nodeLevel) {
        String projectLevelKey = String.format("%s%s%s",
                projectName, Constants.PATTERN_NODE_KEY_DELIMITER, nodeLevel);
        //For java pass object by reference
        Map<String, PatternNode> nodes = new HashMap<>();
        if (patternNodes.containsKey(projectLevelKey)) {
            nodes.putAll(patternNodes.get(projectLevelKey));
        }
        return nodes;
    }

    public Map<String, PatternNode> getNodes(String nodeLevel) {
        Map<String, PatternNode> nodes  = new HashMap<>();
        for (Map.Entry<String, Map<String, PatternNode>> entry: patternNodes.entrySet()) {
            String thisLevel = entry.getKey().split(Constants.PATTERN_NODE_KEY_DELIMITER)[1];
            if (StringUtils.equalsIgnoreCase(nodeLevel, thisLevel)) {
                nodes.putAll(entry.getValue());
            }
        }
        return nodes;
    }

    public String visualize() {
       return "";
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
            stringBuilder.append(entry.getKey());
            stringBuilder.append(System.getProperty("line.separator"));
            for (Map.Entry<String, PatternNode> entryNode : entry.getValue().entrySet()) {
                stringBuilder.append(String.format("key: %s\tvalue:%s", entryNode.getKey(), entryNode.getValue().toString()));
                stringBuilder.append(System.getProperty("line.separator"));
            }
        }
        return stringBuilder.toString();
    }

    public void saveTreeToFile(String fileName) {
        try {
            FileWriter fw = new FileWriter(fileName);
            fw.write(PatternLevelTree.getInstance().toString());
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
