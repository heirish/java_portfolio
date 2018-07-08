package com.company.platform.team.projspark.PatternCursoryFinder;

import com.company.platform.team.projspark.common.data.Constants;
import com.company.platform.team.projspark.common.data.PatternNode;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import com.company.platform.team.projspark.modules.FastClustering;
import com.company.platform.team.projspark.modules.PatternTreeHelper;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

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
    private Map<String, Map<PatternNodeKey, PatternNode>> patternNodes;
    private PatternTreeHelper treeHelper;
    private static PatternLeaves forest = new PatternLeaves();
    private static final Gson gson = new Gson();

    private PatternLeaves() {
        //TODO:recover from local checkpoint
        patternNodes = new HashMap<>();
        treeHelper = new PatternTreeHelper();
        Map<PatternNodeKey, PatternNode> nodes = treeHelper.getAllLeaves();
        //TODO: split nodes by project
    }

    public static PatternLeaves getInstance() {
        return forest;
    }

    //TODO:input a distance function
    public PatternNodeKey getParentNodeId(List<String> tokens, String projectName, double maxDistance) {
        Map<PatternNodeKey, PatternNode> levelNodes = getNodes(projectName);
        if (levelNodes != null) {
            for (Map.Entry<PatternNodeKey, PatternNode> node: levelNodes.entrySet()) {
                if (FastClustering.belongsToCluster(tokens,
                        node.getValue().getRepresentTokens(), maxDistance)) {
                    return node.getKey();
                }
            }
        }

        int triedTimes = 0;
        PatternNode parentNode= new PatternNode(tokens);
        do {
            PatternNodeKey nodeKey = addLeaf(projectName, parentNode);
            if (nodeKey != null) {
                return nodeKey;
            } else {
                Map<PatternNodeKey, PatternNode> newLevelNodes = getNodes(projectName);
                MapDifference<PatternNodeKey, PatternNode> diff = Maps.difference(levelNodes, newLevelNodes);
                for (Map.Entry<PatternNodeKey, PatternNode> node : diff.entriesOnlyOnRight().entrySet()) {
                    if (FastClustering.belongsToCluster(tokens,
                            node.getValue().getRepresentTokens(), maxDistance)) {
                        return node.getKey();
                    }
                    levelNodes.put(node.getKey(), node.getValue());
                }
                triedTimes++;
            }
        } while (triedTimes < Constants.FINDCLUSTER_TOLERANCE_TIMES);

        return null;
    }


    public PatternNodeKey addLeaf(String projectName, PatternNode node) {
        try {
            String nodeId = UUID.randomUUID().toString().replace("-", "");
            PatternNodeKey nodeKey = new PatternNodeKey(projectName, 0);
            if (treeHelper.addNodesToCenter(nodeKey, node)) {
                if (patternNodes.containsKey(nodeKey.getProjectName())) {
                    patternNodes.get(projectName).put(nodeKey, node);
                } else {
                    Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
                    nodes.put(nodeKey, node);
                    patternNodes.put(projectName, nodes);
                }
                saveToFile("tree/patternLeaves");
                return nodeKey;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<PatternNodeKey, PatternNode> getNodes(String projectName) {
        //For java pass object by reference
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        if (patternNodes.containsKey(projectName)) {
            nodes.putAll(patternNodes.get(projectName));
        }
        return nodes;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, Map<PatternNodeKey, PatternNode>> entry : patternNodes.entrySet()) {
            stringBuilder.append("project " + entry.getKey() + " leaves:");
            stringBuilder.append(System.getProperty("line.separator"));
            for (Map.Entry<PatternNodeKey, PatternNode> entryNode : entry.getValue().entrySet()) {
                stringBuilder.append(String.format("key: %s\tvalue:%s",
                        entryNode.getKey().toString(), entryNode.getValue().toString()));
                stringBuilder.append(System.getProperty("line.separator"));
            }
        }
        return stringBuilder.toString();
    }

    private String toJsonString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, Map<PatternNodeKey, PatternNode>> entry : patternNodes.entrySet()) {
            for (Map.Entry<PatternNodeKey, PatternNode> entryNode : entry.getValue().entrySet()) {
                Map<String, String> jsonItems = new HashMap<>();
                jsonItems.put(Constants.FIELD_PATTERNID, entryNode.getKey().toString());
                jsonItems.put(Constants.FIELD_REPRESENTTOKENS,
                        String.join(Constants.PATTERN_NODE_KEY_DELIMITER, entryNode.getValue().getRepresentTokens()));
                jsonItems.put(Constants.FIELD_PATTERNTOKENS,
                        String.join(Constants.PATTERN_NODE_KEY_DELIMITER, entryNode.getValue().getPatternTokens()));
                if (entryNode.getValue().hasParent()) {
                    jsonItems.put("parentId", entryNode.getValue().getParentId().toString());
                } else {
                    jsonItems.put("parentId", "");
                }
                stringBuilder.append(gson.toJson(jsonItems));
                stringBuilder.append(System.getProperty("line.separator"));
            }
        }
        return stringBuilder.toString();
    }


    public void saveToFile(String fileName) {
        try {
            FileWriter fw = new FileWriter(fileName);
            fw.write(PatternLeaves.getInstance().toJsonString());
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
