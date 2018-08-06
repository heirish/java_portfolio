package com.company.platform.team.projpatternreco.stormtopology.data;

import com.company.platform.team.projpatternreco.common.data.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by admin on 2018/6/29.
 * Singletone
 */
public final class PatternNodes {
    private static final Logger logger = LoggerFactory.getLogger(PatternNodes.class);

    private ConcurrentHashMap<PatternLevelKey, ConcurrentHashMap<PatternNodeKey, PatternNode>> patternNodes;

    public PatternNodes() {
            patternNodes = new ConcurrentHashMap<>();
    }


    public void addNode(PatternNodeKey nodeKey, PatternNode node ) {
        if (nodeKey != null) {
            if (patternNodes.containsKey(nodeKey.getLevelKey())) {
                patternNodes.get(nodeKey.getLevelKey()).put(nodeKey, node);
            } else {
                ConcurrentHashMap<PatternNodeKey, PatternNode> nodes = new ConcurrentHashMap<>();
                nodes.put(nodeKey, node);
                patternNodes.put(nodeKey.getLevelKey(), nodes);
            }
            logger.debug("add node to PatternNodes, key:" + nodeKey.getLevelKey() + ", " + node.toString());
        }
    }

    public void addNodes(Map<PatternNodeKey, PatternNode> nodes) {
        if (nodes != null) {
            for (Map.Entry<PatternNodeKey, PatternNode> node : nodes.entrySet() ) {
               addNode(node.getKey(), node.getValue());
            }
        }
    }

    public void deleteNode(PatternNodeKey nodeKey) {
        if (nodeKey != null) {
            PatternLevelKey levelKey = nodeKey.getLevelKey();
            if (patternNodes.containsKey(levelKey)) {
                Map<PatternNodeKey, PatternNode> nodes = patternNodes.get(levelKey);
                if (nodes.containsKey(nodeKey)) {
                    nodes.remove(nodeKey);
                }
            }
        }
    }

    public void deleteLevelNodes(PatternLevelKey levelKey) {
        if (patternNodes.containsKey(levelKey)) {
            patternNodes.remove(levelKey);
        }

        if (patternNodes.containsKey(levelKey)) {
            logger.info("delete levelKey " + levelKey.toString() + "failed.");
        } else {
            logger.info("delete levelKey " + levelKey.toString() + "success.");
        }
    }

    public void deleteProjectNodes(String projectName) {
        if (!StringUtils.isEmpty(projectName)) {
            for (PatternLevelKey key : patternNodes.keySet()) {
                if (StringUtils.equals(key.getProjectName(), projectName)) {
                    deleteLevelNodes(key);
                }
            }
        }
    }

    public void updateNode(PatternNodeKey nodeKey, PatternNode node) {
        if (nodeKey != null) {
            if (patternNodes.containsKey(nodeKey.getLevelKey())) {
                patternNodes.get(nodeKey.getLevelKey()).put(nodeKey, node);
            } else {
                ConcurrentHashMap<PatternNodeKey, PatternNode> nodes = new ConcurrentHashMap<>();
                nodes.put(nodeKey, node);
                patternNodes.put(nodeKey.getLevelKey(), nodes);
            }
            logger.debug("update node key:" + nodeKey.getLevelKey() + ", " + node.toString());
        }
    }

    public PatternNode getNode(PatternNodeKey nodeKey) {
        if (!patternNodes.containsKey(nodeKey.getLevelKey())) {
            logger.error("can not find level nodes of key: " + nodeKey.toString());
            return null;
        }

        if (!(patternNodes.get(nodeKey.getLevelKey()).containsKey(nodeKey))) {
            logger.error("can not find node of key: " + nodeKey.toString());
            return null;
        }
        return patternNodes.get(nodeKey.getLevelKey()).get(nodeKey);
    }

    public Map<PatternNodeKey, PatternNode> getNodes(PatternLevelKey levelKey) {
        if (patternNodes.containsKey(levelKey)) {
            return patternNodes.get(levelKey);
        }

        return null;
    }

    public PatternNodeKey getNodeKeyByRepresentTokens(PatternLevelKey levelKey, List<String> representTokens) {
        if (patternNodes.containsKey(levelKey)) {
            for (Map.Entry<PatternNodeKey, PatternNode> node : patternNodes.get(levelKey).entrySet()) {
                if (node.getValue().getRepresentTokens().equals(representTokens)) {
                    return node.getKey();
                }
            }
        }
        return null;
    }

    public Set<PatternNodeKey> getNodeKeys(PatternLevelKey levelKey) {
        if (patternNodes.containsKey(levelKey)) {
            return patternNodes.get(levelKey).keySet();
        }
        return null;
    }

    public int getLevelNodeSize(PatternLevelKey levelKey) {
        if (patternNodes.containsKey(levelKey)) {
            return patternNodes.get(levelKey).size();
        }
        return 0;
    }

    public Set<String> getAllProjectsName() {
        Set<String> projectList = new HashSet<>();
        for (PatternLevelKey key : patternNodes.keySet()) {
            projectList.add(key.getProjectName());
        }
        return projectList;
    }

    private int getProjectMaxLevel(String name) {
        int maxLevel = -1;
        for (PatternLevelKey key: patternNodes.keySet()) {
            if (StringUtils.equals(key.getProjectName(), name)) {
                int level = key.getLevel();
                maxLevel = level > maxLevel ? level : maxLevel;
            }
        }
        return maxLevel;
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
            if (nodes == null || nodes.size() == 0) {
                continue;
            }
            logger.debug(String.format("found %s nodes for level %s.", nodes.size(), i));
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
                    logger.warn("not found node " + entry.getKey() + "'s parent in visual tree or it has not parent");
                }
            }
        }
        return root.visualize();
    }

    public String toString(String projectName) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<PatternLevelKey, ConcurrentHashMap<PatternNodeKey, PatternNode>> entry : patternNodes.entrySet()) {
            if (!StringUtils.equals(entry.getKey().getProjectName(), projectName)) {
                continue;
            }
            for (Map.Entry<PatternNodeKey, PatternNode> entryNode : entry.getValue().entrySet()) {
                stringBuilder.append(entryNode.getKey().toString() + " : ");
                stringBuilder.append(entryNode.getValue().toJson());
                stringBuilder.append(System.getProperty("line.separator"));
                stringBuilder.append(System.getProperty("line.separator"));
            }
        }
        return stringBuilder.toString();
    }
}
