package com.company.platform.team.projpatternreco.sparkandhadoop.patternerfiner;

import com.company.platform.team.projpatternreco.common.data.*;
import com.company.platform.team.projpatternreco.modules.*;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
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
    private static PatternLevelTree forest = new PatternLevelTree();
    private static final Gson gson = new Gson();


    public static synchronized PatternLevelTree getInstance() {
        return forest;
    }

    private PatternLevelTree() {
        //TODO:recover from local checkpoint
        patternNodes = new HashMap<>();
        try {
            Map<PatternNodeKey, PatternNode> nodes = readFromFile("tree/patternLeaves");
            System.out.println("get " + nodes.size() + " nodes from file tree/patternLeaves");
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
            //The last
            if (keys.size() > 0 && projectLevelNodes != null && projectLevelNodes.size() > 0) {
                patternNodes.put(keys.last().getLevelKey(), projectLevelNodes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<PatternNodeKey, PatternNode> readFromFile(String fileName) throws Exception {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    Map<String, String> fields = gson.fromJson(line, Map.class);
                    PatternNodeKey key = PatternNodeKey.fromString(fields.get(Constants.FIELD_PATTERNID));
                    List<String> patternTokens = Arrays.asList(fields.get(Constants.FIELD_PATTERNTOKENS)
                            .split(Constants.PATTERN_NODE_KEY_DELIMITER));
                    List<String> representTokens = Arrays.asList(fields.get(Constants.FIELD_REPRESENTTOKENS)
                            .split(Constants.PATTERN_NODE_KEY_DELIMITER));
                    PatternNode node = new PatternNode(representTokens);
                    node.updatePatternTokens(patternTokens);
                    String parentKeyString = fields.get("parentId");
                    if (!StringUtils.isEmpty(parentKeyString)) {
                        node.setParent(PatternNodeKey.fromString(parentKeyString));
                    }
                    nodes.put(key, node);
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return nodes;
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
        PatternNodeKey nodeKey = new PatternNodeKey(projectName, nodeLevel);
        PatternNode parentNode= new PatternNode(tokens);
        do {
            long timestamp = addNode(nodeKey, parentNode, getMaxUpdatedTime(projectName, nodeLevel));
            if (timestamp > 0) {
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

    public long updateNode(PatternNodeKey nodeKey, PatternNode node) {
        PatternLevelKey levelKey = nodeKey.getLevelKey();
        if (!patternNodes.containsKey(levelKey)
                || !patternNodes.get(levelKey).containsKey(nodeKey)) {
            return 0;
        }

        node.setLastupdatedTime(System.currentTimeMillis());
        patternNodes.get(levelKey).put(nodeKey, node);
        return node.getLastupdatedTime();
    }

    public long addNode(PatternNodeKey nodeKey, PatternNode node, long clientlastUpdatedTime) {

        long maxUpdateTime = getMaxUpdatedTime(nodeKey.getProjectName(),
                nodeKey.getLevel());
        //client need to synchronize
        if (clientlastUpdatedTime < maxUpdateTime) {
            return 0;
        }
        node.setLastupdatedTime(System.currentTimeMillis());

        PatternLevelKey levelKey = nodeKey.getLevelKey();
        if (patternNodes.containsKey(nodeKey.getLevelKey())) {
            patternNodes.get(levelKey).put(nodeKey, node);
        } else {
            Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
            nodes.put(nodeKey, node);
            patternNodes.put(levelKey, nodes);
        }

        return node.getLastupdatedTime();
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
        System.out.println("get levelKey from client: " + levelKey.toString());
        if (patternNodes.containsKey(levelKey)) {
            nodes.putAll(patternNodes.get(levelKey));
        }
        return nodes;
    }

    public Map<PatternNodeKey, PatternNode> getNodesNewerThan(String projectName,
                                                              int nodeLevel,
                                                              long lastUpdatedTime) {
        Map<PatternNodeKey, PatternNode> returnNodes = new HashMap<>();
        PatternLevelKey levelKey = new PatternLevelKey(projectName, nodeLevel);
        if (patternNodes.containsKey(levelKey)) {
            for (Map.Entry<PatternNodeKey, PatternNode> entry : patternNodes.get(levelKey).entrySet()) {
                if (entry.getValue().getLastupdatedTime() >= lastUpdatedTime) {
                    returnNodes.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return returnNodes;
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

    private long getMaxUpdatedTime(String projectName, int nodeLevel) {
        long maxUpdateTime = 0;
        PatternLevelKey levelKey = new PatternLevelKey(projectName, nodeLevel);
        for (Map.Entry<PatternLevelKey, Map<PatternNodeKey, PatternNode>> entry : patternNodes.entrySet()) {
            for (Map.Entry<PatternNodeKey, PatternNode> nodeEntry : entry.getValue().entrySet()) {
                if (levelKey.equals(nodeEntry.getKey().getLevelKey())) {
                    maxUpdateTime = maxUpdateTime < nodeEntry.getValue().getLastupdatedTime() ?
                            nodeEntry.getValue().getLastupdatedTime() : maxUpdateTime;
                }
            }
        }
        return maxUpdateTime;
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

    public void saveTreeToFile(String fileName, String projectName) {
        try {
            FileWriter fw = new FileWriter(fileName);
            String treeString = StringUtils.isEmpty(projectName) ?  visualize() : visualize(projectName);
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
                    if (entryNode.getValue().hasParent()) {
                        jsonItems.put("parentId", entryNode.getValue().getParentId().toString());
                    } else {
                        jsonItems.put("parentId", "");
                    }
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
