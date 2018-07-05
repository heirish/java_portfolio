package com.company.platform.team.projspark.data;

import com.company.platform.team.projspark.modules.FastClustering;
import com.company.platform.team.projspark.modules.PatternTreeHelper;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
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
    private static final Gson gson = new Gson();

    private PatternLevelTree() {
        //TODO:recover from local checkpoint
        patternNodes = new HashMap<>();
        treeHelper = new PatternTreeHelper();
        Map<String, PatternNode> nodes = treeHelper.getAllNodes();
        //split nodes by project+level
        SortedSet<String> keys = new TreeSet<>(nodes.keySet());
        String lastProjectLevel = "";
        Map<String, PatternNode> projectLevelNodes = new HashMap<>();
        for (String key : keys) {
            String thisProjectLevelKey = key.substring(0, key.lastIndexOf(Constants.PATTERN_NODE_KEY_DELIMITER));
            if (!StringUtils.equalsIgnoreCase(lastProjectLevel, thisProjectLevelKey)) {
                projectLevelNodes = new HashMap<>();
                lastProjectLevel = thisProjectLevelKey;
                patternNodes.put(lastProjectLevel, projectLevelNodes);
            }
            projectLevelNodes.put(key, nodes.get(key));
        }
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
                System.out.println("add node: " + nodeKey);
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
        Map<String, PatternNode> nodes = getNodes(projectName, nodeLevel);
        if (nodes != null && nodes.containsKey(nodeKey)) {
            return new PatternNode(nodes.get(nodeKey));
        } else {
            return null;
        }
    }

    public PatternNode getNode(String projectName, int nodeLevel, String nodeId) {
        String nodeKey = String.format("%s%s%s%s%s",
                projectName, Constants.PATTERN_NODE_KEY_DELIMITER,
                nodeLevel, Constants.PATTERN_NODE_KEY_DELIMITER,
                nodeId);
        return getNode(nodeKey);
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

    public Set<String> getAllProjectsName() {
       Set<String> projectList = new HashSet<>();
       for (String key : patternNodes.keySet()) {
           String[] keyItems = key.split(Constants.PATTERN_NODE_KEY_DELIMITER);
           System.out.println(Arrays.toString(keyItems));
           if (keyItems.length == 2) {
               projectList.add(keyItems[0]);
           }
       }
       return projectList;
    }

    public int getProjectMaxLevel(String name) {
        int maxLevel = -1;
        for (String key : patternNodes.keySet()) {
            String[] keyItems = key.split(Constants.PATTERN_NODE_KEY_DELIMITER);
            if (keyItems.length == 2 && Integer.parseInt(keyItems[1]) > maxLevel) {
                int level = Integer.parseInt(keyItems[1]);
                maxLevel = level > maxLevel ? level : maxLevel;
            }
        }
        return maxLevel;
    }

    public String visualize() {
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> projectNames = getAllProjectsName();
        for (String name : projectNames) {
            System.out.println(name);
            stringBuilder.append(visualize(name));
        }
        return stringBuilder.toString();
    }

    public String visualize(String name) {
        VisualTreeNode root = new VisualTreeNode("root", name);
        Map<String, PatternNode> nodes;
        int maxLevel = getProjectMaxLevel(name);
        if (maxLevel < 0) {
            return "";
        }
        for (int i=maxLevel; i>=0; i--) {
            nodes = getNodes(name, String.valueOf(i));
            if (nodes == null) {
                continue;
            }
            System.out.println(String.format("found %s nodes for level %s.", nodes.size(), i));
            for (Map.Entry<String, PatternNode> entry : nodes.entrySet()) {
                String patternString = String.join("", entry.getValue().getPatternTokens());
                //if nodes is not the highest level and don't have parent, drop it
                VisualTreeNode parent = null;
                if (i == maxLevel) {
                    parent = root;
                } else if (entry.getValue().hasParent()) {
                    parent = root.getNode(entry.getValue().getParentId());
                }
                if (parent != null) {
                    parent.addChild(new VisualTreeNode(entry.getKey(), patternString));
                } else {
                    System.out.println("not found node " + entry.getKey() + "'s parent in visual tree or it has not parent");
                }
            }
        }
        return root.visualize();
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
            String treeString = visualize();
            System.out.println(treeString);
            fw.write(treeString);
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void backupTree(String fileName) {
        try {
            FileWriter fw = new FileWriter(fileName);
            for (Map.Entry<String, Map<String, PatternNode>> entry : patternNodes.entrySet()) {
                for (Map.Entry<String, PatternNode> entryNode : entry.getValue().entrySet()) {
                    Map<String, String> jsonItems = new HashMap<>();
                    jsonItems.put(Constants.FIELD_PATTERNID, entryNode.getKey());
                    jsonItems.put(Constants.FIELD_REPRESENTTOKENS,
                            String.join(Constants.PATTERN_NODE_KEY_DELIMITER, entryNode.getValue().getRepresentTokens()));
                    jsonItems.put(Constants.FIELD_PATTERNTOKENS,
                            String.join(Constants.PATTERN_NODE_KEY_DELIMITER, entryNode.getValue().getPatternTokens()));
                    jsonItems.put("parentId", entryNode.getValue().getParentId());
                    try {
                        fw.write(gson.toJson(jsonItems) + System.getProperty("line.separator"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
