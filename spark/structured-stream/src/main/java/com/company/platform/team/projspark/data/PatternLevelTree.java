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
    private Map<PatternLevelKey, Map<PatternNodeKey, PatternNode>> patternNodes;
    private PatternTreeHelper treeHelper;
    private static PatternLevelTree forest = new PatternLevelTree();
    private static final Gson gson = new Gson();

    private PatternLevelTree() {
        //TODO:recover from local checkpoint
        patternNodes = new HashMap<>();
        treeHelper = new PatternTreeHelper();
        Map<PatternNodeKey, PatternNode> nodes = treeHelper.getAllNodes();
        //split nodes by LevelKey
        SortedSet<PatternNodeKey> keys = new TreeSet<>(nodes.keySet());
        PatternLevelKey lastLevelKey = null;
        Map<PatternNodeKey, PatternNode> projectLevelNodes = new HashMap<>();
        for (PatternNodeKey key : keys) {
            if (!key.getLevelKey().equals(lastLevelKey) && lastLevelKey != null) {
                patternNodes.put(lastLevelKey, projectLevelNodes);
                lastLevelKey = key;
                projectLevelNodes = new HashMap<>();
            }
            projectLevelNodes.put(key, nodes.get(key));
        }
    }

    public static PatternLevelTree getInstance() {
        return forest;
    }

    //TODO:input a distance function
    public PatternNodeKey getParentNodeId(List<String> tokens, String projectName, int nodeLevel, double maxDistance) {
        PatternLevelKey levelKey = new PatternLevelKey(projectName, nodeLevel);
        Map<PatternNodeKey, PatternNode> levelNodes = getNodes(levelKey);
        if (levelNodes != null) {
            for (Map.Entry<PatternNodeKey, PatternNode> node: levelNodes.entrySet()) {
                if (FastClustering.belongsToCluster(tokens, node.getValue().getRepresentTokens(), maxDistance)) {
                    return node.getKey();
                }
            }
        }

        int triedTimes = 0;
        PatternNode parentNode= new PatternNode(tokens);
        do {
            PatternNodeKey nodeKey = addNode(projectName, nodeLevel, parentNode);
            if (nodeKey != null) {
                return nodeKey;
            } else {
                Map<PatternNodeKey, PatternNode> newLevelNodes = getNodes(levelKey);
                MapDifference<PatternNodeKey, PatternNode> diff = Maps.difference(levelNodes, newLevelNodes);
                for (Map.Entry<PatternNodeKey, PatternNode> node : diff.entriesOnlyOnRight().entrySet()) {
                    if (FastClustering.belongsToCluster(tokens, node.getValue().getRepresentTokens(), maxDistance)) {
                        return node.getKey();
                    }
                    levelNodes.put(node.getKey(), node.getValue());
                }
                triedTimes++;
            }
        } while (triedTimes < Constants.FINDCLUSTER_TOLERANCE_TIMES);

        return null;
    }

    public void updateNode(PatternNodeKey nodeKey, PatternNode node) throws Exception{
        PatternLevelKey levelKey = nodeKey.getLevelKey();
        if (patternNodes.containsKey(levelKey) && patternNodes.get(levelKey).containsKey(nodeKey)) {
            if(treeHelper.updateNodesToCenter(nodeKey, node)) {
                patternNodes.get(levelKey).put(nodeKey, node);
            } else {
                throw new Exception("udpate to Nodes center failed");
            }
        } else {
            throw new Exception("node not exists");
        }
    }

    public PatternNodeKey addNode(String projectName, int nodeLevel, PatternNode node) {
        try {
            String nodeId = UUID.randomUUID().toString().replace("-", "");
            PatternNodeKey nodeKey = new PatternNodeKey(projectName, nodeLevel, nodeId);
            if (treeHelper.addNodesToCenter(nodeKey, node)) {
                PatternLevelKey levelKey = nodeKey.getLevelKey();
                System.out.println("add node: " + nodeKey.toString());
                if (patternNodes.containsKey(levelKey)) {
                    patternNodes.get(levelKey).put(nodeKey, node);
                } else {
                    Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
                    nodes.put(nodeKey, node);
                    patternNodes.put(levelKey, nodes);
                }
                return nodeKey;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public PatternNode getNode(PatternNodeKey nodeKey) {
        Map<PatternNodeKey, PatternNode> nodes = getNodes(nodeKey.getLevelKey());
        if (nodes != null && nodes.containsKey(nodeKey)) {
            return new PatternNode(nodes.get(nodeKey));
        } else {
            return null;
        }
    }

    public Map<PatternNodeKey, PatternNode> getNodes(PatternLevelKey levelKey) {
        //For java pass object by reference
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        if (patternNodes.containsKey(levelKey)) {
            nodes.putAll(patternNodes.get(levelKey));
        }
        return nodes;
    }

    public Map<PatternNodeKey, PatternNode> getNodes(int nodeLevel) {
        Map<PatternNodeKey, PatternNode> nodes  = new HashMap<>();
        for (Map.Entry<PatternLevelKey, Map<PatternNodeKey, PatternNode>> entry: patternNodes.entrySet()) {
            if (entry.getKey().getLevel() == nodeLevel) {
                nodes.putAll(entry.getValue());
            }
        }
        return nodes;
    }

    public Set<String> getAllProjectsName() {
       Set<String> projectList = new HashSet<>();
       for (PatternLevelKey key : patternNodes.keySet()) {
               projectList.add(key.getProjectName());
       }
       return projectList;
    }

    public int getProjectMaxLevel(String name) {
        int maxLevel = -1;
        for (PatternLevelKey key : patternNodes.keySet()) {
            if (StringUtils.equals(key.getProjectName(), name)) {
                int level = key.getLevel();
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
        Map<PatternNodeKey, PatternNode> nodes;
        int maxLevel = getProjectMaxLevel(name);
        if (maxLevel < 0) {
            return "";
        }
        for (int i=maxLevel; i>=0; i--) {
            nodes = getNodes(new PatternLevelKey(name, i));
            if (nodes == null) {
                continue;
            }
            System.out.println(String.format("found %s nodes for level %s.", nodes.size(), i));
            for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
                String patternString = entry.getKey().toString() + " : " + String.join("", entry.getValue().getPatternTokens());
                //if nodes is not the highest level and don't have parent, drop it
                VisualTreeNode parent = null;
                if (i == maxLevel) {
                    parent = root;
                } else if (entry.getValue().hasParent()) {
                    parent = root.getNode(entry.getValue().getParentId().toString());
                }
                if (parent != null) {
                    parent.addChild(new VisualTreeNode(entry.getKey().toString(), patternString));
                } else {
                    System.out.println("not found node " + entry.getKey() + "'s parent in visual tree or it has not parent");
                }
            }
        }
        return root.visualize();
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<PatternLevelKey, Map<PatternNodeKey, PatternNode>> entry : patternNodes.entrySet()) {
            stringBuilder.append(entry.getKey().toString());
            stringBuilder.append(System.getProperty("line.separator"));
            for (Map.Entry<PatternNodeKey, PatternNode> entryNode : entry.getValue().entrySet()) {
                stringBuilder.append(String.format("key: %s\tvalue:%s", entryNode.getKey().toString(), entryNode.getValue().toString()));
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
            for (Map.Entry<PatternLevelKey, Map<PatternNodeKey, PatternNode>> entry : patternNodes.entrySet()) {
                for (Map.Entry<PatternNodeKey, PatternNode> entryNode : entry.getValue().entrySet()) {
                    Map<String, String> jsonItems = new HashMap<>();
                    jsonItems.put(Constants.FIELD_PATTERNID, entryNode.getKey().toString());
                    jsonItems.put(Constants.FIELD_REPRESENTTOKENS,
                            String.join(Constants.PATTERN_NODE_KEY_DELIMITER, entryNode.getValue().getRepresentTokens()));
                    jsonItems.put(Constants.FIELD_PATTERNTOKENS,
                            String.join(Constants.PATTERN_NODE_KEY_DELIMITER, entryNode.getValue().getPatternTokens()));
                    jsonItems.put("parentId", entryNode.getValue().getParentId().toString());
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
