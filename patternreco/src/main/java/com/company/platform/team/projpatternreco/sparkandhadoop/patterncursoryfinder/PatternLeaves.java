package com.company.platform.team.projpatternreco.sparkandhadoop.patterncursoryfinder;

import com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper.PatternNodeCenterType;
import com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper.PatternNodeClient;
import com.company.platform.team.projpatternreco.common.data.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.modules.FastClustering;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/29.
 * Singletone
 */
// TODO:thread safe
public final class PatternLeaves {
    //Map<project, Map<project-level-nodeid, PatternNode>>
    private static PatternNodeClient client = new PatternNodeClient("localhost:7911",
                                         PatternNodeCenterType.HDFS);
    private Map<String, Map<PatternNodeKey, PatternNode>> patternNodes;
    private static PatternLeaves forest = new PatternLeaves();
    private static final Gson gson = new Gson();

    private PatternLeaves() {
        //TODO:recover from local checkpoint
        patternNodes = new HashMap<>();
    }

    public static synchronized PatternLeaves getInstance() {
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
        PatternNodeKey nodeKey = new PatternNodeKey(projectName, 0);
        do {
            long timestamp = addLeaf(nodeKey, parentNode);
            if (timestamp > 0) {
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

    private long getMaxUpdatedTime(String projectName) {
        long maxUpdateTime = 0;
            for (Map.Entry<PatternNodeKey, PatternNode> nodeEntry : patternNodes.get(projectName).entrySet()) {
                    maxUpdateTime = maxUpdateTime < nodeEntry.getValue().getLastupdatedTime() ?
                            nodeEntry.getValue().getLastupdatedTime() : maxUpdateTime;
            }
        return maxUpdateTime;
    }


    public long addLeaf(PatternNodeKey nodeKey, PatternNode node) {
        long updatedTimestamp= 0;
        try {
            String projectName = nodeKey.getProjectName();
            long localLastUpdatedTime = getMaxUpdatedTime(projectName);
            updatedTimestamp = client.addNode(nodeKey, node, localLastUpdatedTime);
            node.setLastupdatedTime(updatedTimestamp);
            if (updatedTimestamp > 0) {
                if (patternNodes.containsKey(nodeKey.getProjectName())) {
                    patternNodes.get(projectName).put(nodeKey, node);
                } else {
                    Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
                    nodes.put(nodeKey, node);
                    patternNodes.put(projectName, nodes);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return updatedTimestamp;
    }

    public Map<PatternNodeKey, PatternNode> getNodes(String projectName) {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        if (!patternNodes.containsKey(projectName)) {
            try {
                Map<PatternNodeKey, PatternNode> nodesFromCenter = client.getNodes(projectName, 0);
                if (nodesFromCenter != null) {
                    patternNodes.put(projectName, nodesFromCenter);
                }
                nodes.putAll(patternNodes.get(projectName));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
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
                        String.join(Constants.PATTERN_TOKENS_DELIMITER, entryNode.getValue().getRepresentTokens()));
                jsonItems.put(Constants.FIELD_PATTERNTOKENS,
                        String.join(Constants.PATTERN_TOKENS_DELIMITER, entryNode.getValue().getPatternTokens()));
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
