package com.company.platform.team.projspark.data;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Singleton
 */
public class PatternTree {
    private Map<Integer, Map<String, PatternNode>> tree;

    public PatternTree() {
        tree = new HashMap<>();
    }

    public Map<String, PatternNode> getNodes(Integer nodeLevel) {
        return tree.get(nodeLevel);
    }

    public PatternNode getNode(Integer nodeLevel, String nodeId) {
        return tree.get(nodeLevel).get(nodeId);
    }

    public boolean exists(Integer nodeLevel, String nodeId) {
        return tree.containsKey(nodeLevel) && tree.get(nodeLevel).containsKey(nodeId);
    }

    public String requestNewNodeId(int nodeLevel) {
        //TODO: request from the NodeId center
        // if the max Id is greater than local, then synchronize
        return UUID.randomUUID().toString().replace("-", "");
    }

    public String addNode(int nodeLevel, PatternNode node) {
        String nodeId = requestNewNodeId(nodeLevel);
        setNode(nodeLevel, nodeId, node);
        return nodeId;
    }

    public void setNode(int nodeLevel, String nodeId, PatternNode node) {
        if (tree.containsKey(nodeLevel)) {
            tree.get(nodeLevel).put(nodeId, node);
        } else {
            Map<String, PatternNode> nodeMap = new HashMap<>();
            nodeMap.put(nodeId, node);
            tree.put(new Integer(nodeLevel), nodeMap);
        }
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<Integer, Map<String, PatternNode>> entry : tree.entrySet()) {
           stringBuilder.append(String.format("level: %d,", entry.getKey()));
            stringBuilder.append(System.getProperty("line.separator"));
           for (Map.Entry<String, PatternNode> entryNode: entry.getValue().entrySet()) {
               stringBuilder.append(String.format("\t\tnodeId: %s, nodeInfo: %s",
                       entryNode.getKey(), entryNode.getValue().toString()));
               stringBuilder.append(System.getProperty("line.separator"));
           }
        }
        return stringBuilder.toString();
    }
}
