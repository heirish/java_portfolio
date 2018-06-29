package com.company.platform.team.projspark.data;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/29.
 * Singletone
 */
// TODO:thread safe
public final class PatternForest {
    private static Map<String, PatternNode> patternNodes= new HashMap<>();
    private static PatternForest forest = new PatternForest();
    //TODO:
    //add Map<proj, Map<level, nodeId>> to fast the getNodes

    private String formatPatternNodeKey(String projectName, int level, String nodeId) {
        return String.format("%s%s%s%s%s",
                projectName, Constants.PATTERN_NODE_KEY_DELIMITER,
                level, Constants.PATTERN_NODE_KEY_DELIMITER,
                nodeId);
    }

    public static PatternForest getInstance() {
        return forest;
    }

    private static String getNodeId() {
        return "";
    }

    public PatternNode getNode(String name, int nodeLevel, String nodeId) {
        String nodeKey = formatPatternNodeKey(name, nodeLevel, nodeId);
        return patternNodes.get(nodeKey);
    }

    public Map<String, PatternNode> getNodes(String name, int nodeLevel) {
        String nodeKeyMatch = String.format("%s%s%s%s",
                name,Constants.PATTERN_NODE_KEY_DELIMITER,
                nodeLevel, Constants.PATTERN_NODE_KEY_DELIMITER);

        return getNodesKeyLike(nodeKeyMatch);
    }

    public Map<String, PatternNode> getNodes(String name) {
        String nodeKeyMatch = String.format("%s%s%s%s",
                name,Constants.PATTERN_NODE_KEY_DELIMITER);
        return getNodesKeyLike(nodeKeyMatch);
    }

    private Map<String, PatternNode> getNodesKeyLike(String keyMatch) {
        Map<String, PatternNode> nodes = new HashMap<>();
        for(Map.Entry<String, PatternNode> entry : patternNodes.entrySet()) {
            if (entry.getKey().startsWith(keyMatch)) {
                nodes.put(entry.getKey(), entry.getValue());
            }
        }
        return nodes;
    }

    public String addNode(String name, int nodeLevel, PatternNode node) {
        //TODO:getNodeId failed
        String nodeId = getNodeId();
        String nodeKey = formatPatternNodeKey(name, nodeLevel, nodeId);
        patternNodes.put(nodeKey, node);
        return nodeId;
    }


    public void updateNodePattern(String name, int nodeLevel, String nodeId, List<String> pattern) {
        String nodeKey = formatPatternNodeKey(name, nodeLevel, nodeId);
        PatternNode node = patternNodes.get(nodeKey);
        node.updatePatternTokens(pattern);
        patternNodes.put(nodeKey, node);
    }

    public void setNodeParent(String name, int nodeLevel, String nodeId, String parentId) {
        String nodeKey = formatPatternNodeKey(name, nodeLevel, nodeId);
        PatternNode node = patternNodes.get(nodeKey);
        node.setParent(parentId);
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
        for (Map.Entry<String, PatternNode> entry : patternNodes.entrySet()) {
            stringBuilder.append(String.format("%s\t%s", entry.getKey(), entry.getValue().toString()));
            stringBuilder.append(System.getProperty("line.separator"));
        }
        return stringBuilder.toString();
    }

    private static void saveTreeToFile(String fileName) {
        try {
            FileWriter fw = new FileWriter(fileName);
            fw.write(PatternForest.getInstance().toString());
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
