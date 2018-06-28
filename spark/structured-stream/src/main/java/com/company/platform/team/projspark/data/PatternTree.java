package com.company.platform.team.projspark.data;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by admin on 2018/6/21.
 */
public class PatternTree {
    private String projectName;
    private Map<Integer, Map<String, PatternNode>> tree;

    public PatternTree(String projectName) {
        this.projectName = projectName;
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

    public void addNode(int nodeLevel, PatternNode node) throws Exception {
        String nodeId = node.getNodeId();
        if (StringUtils.isEmpty(nodeId)) {
            throw new Exception("Invalid nodeId");
        }

        if (tree.containsKey(nodeLevel)) {
            tree.get(nodeLevel).put(nodeId, node);
        } else {
            Map<String, PatternNode> nodeMap = new HashMap<>();
            nodeMap.put(nodeId, node);
            tree.put(new Integer(nodeLevel), nodeMap);
        }
    }

    public String addNode(int nodeLevel, List<String> representTokens) {
        String nodeId = requestNewNodeId(nodeLevel);
        PatternNode node = new PatternNode(nodeId, representTokens);
        try {
            addNode(nodeLevel, node);
            return nodeId;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
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
