package com.company.platform.team.projpatternreco.stormtopology.refinder;

import com.company.platform.team.projpatternreco.common.data.*;
import com.company.platform.team.projpatternreco.common.modules.FastClustering;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by admin on 2018/6/29.
 * Singletone
 */
// TODO:thread safe
public final class PatternNodes {
    private static final Gson gson = new Gson();
    private static final Logger logger = LoggerFactory.getLogger(PatternNodes.class);

    private ConcurrentHashMap<PatternLevelKey, Map<PatternNodeKey, PatternNode>> patternNodes;
    private static PatternNodes forest;

    public static synchronized PatternNodes getInstance() {
        if (forest == null) {
            forest = new PatternNodes();
        }
        return forest;
    }

    private PatternNodes() {
        try {
            patternNodes = new ConcurrentHashMap<>();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public PatternNodeKey getParentNodeId(List<String> tokens,
                                          PatternLevelKey levelKey,
                                          double maxDistance,
                                          int retryTolerence) {
        Map<PatternNodeKey, PatternNode> levelNodes = getNodes(levelKey);
        PatternNodeKey nodeKey = findNodeIdFromNodes(tokens, levelNodes, maxDistance);
        if (nodeKey != null) {
            return nodeKey;
        }

        int triedTimes = 0;
        do {
            Map<PatternNodeKey, PatternNode> newLevelNodes = getNodes(levelKey);
            MapDifference<PatternNodeKey, PatternNode> diff = Maps.difference(levelNodes, newLevelNodes);
            nodeKey = findNodeIdFromNodes(tokens, diff.entriesOnlyOnRight(), maxDistance);
            if (nodeKey != null) {
                return nodeKey;
            }
            triedTimes++;
        } while (triedTimes < retryTolerence);

        logger.warn("Already tried " + retryTolerence + " times, still can't find parent id for given tokens.");
        return null;
    }

    public Pair<PatternNodeKey, List<String>> mergePatternToNode(PatternNodeKey key,
                                                                 List<String> patternTokens,
                                                                 double maxDist) {
        try {
            PatternNode parentNode = getNode(key);
            List<String> mergedTokens = Aligner.retrievePattern(parentNode.getPatternTokens(), patternTokens);
            if (!mergedTokens.equals(parentNode.getPatternTokens())) {
                parentNode.updatePatternTokens(mergedTokens);
                PatternLevelKey levelKey = new PatternLevelKey(key.getProjectName(), key.getLevel()+1);
                if (!parentNode.hasParent()) {
                    PatternNodeKey grandNodeKey = getParentNodeId(mergedTokens, levelKey, maxDist, 1);
                    if (grandNodeKey == null) {
                        grandNodeKey = new PatternNodeKey(levelKey);
                        addNode(grandNodeKey, new PatternNode(mergedTokens));
                    }
                    parentNode.setParent(grandNodeKey);
                }
                //update the tree node(parent Id and pattern) by key
                if (parentNode.hasParent() && updateNode(key, parentNode)) {
                    return Pair.of(parentNode.getParentId(), mergedTokens);
                }
            } else {
                logger.debug("No need to update node [" + key.toString() + "] pattern.");
            }
        } catch (Exception e) {
            logger.warn("Failed to merged tokens to it's parent pattern.", e);
        }
        return null;
    }

    private PatternNodeKey findNodeIdFromNodes(List<String> tokens,
                                               Map<PatternNodeKey, PatternNode> levelNodes,
                                               double maxDistance) {
        if (levelNodes != null) {
            for (Map.Entry<PatternNodeKey, PatternNode> node: levelNodes.entrySet()) {
                if (FastClustering.belongsToCluster(tokens, node.getValue().getRepresentTokens(), maxDistance)) {
                    return node.getKey();
                }
            }
        }
        return null;
    }

    public Set<String> getAllProjectsName() {
        Set<String> projectList = new HashSet<>();
        for (PatternLevelKey key : patternNodes.keySet()) {
            projectList.add(key.getProjectName());
        }
        return projectList;
    }

    public boolean addNode(PatternNodeKey nodeKey, PatternNode node ) {
        if(setNodeToCenter(nodeKey, node)) {
            if (patternNodes.containsKey(nodeKey.getLevelKey())) {
                patternNodes.get(nodeKey.getLevelKey()).put(nodeKey, node);
            } else {
                Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
                nodes.put(nodeKey, node);
                patternNodes.put(nodeKey.getLevelKey(), nodes);
            }
            return true;
        }
        return false;
    }

    private boolean updateNode(PatternNodeKey nodeKey, PatternNode node) {
        if (!patternNodes.containsKey(nodeKey.getLevelKey())) {
            Map<PatternNodeKey, PatternNode> nodesFromCenter = getNodesFromCenter(nodeKey.getLevelKey());
            if (nodesFromCenter == null) {
                logger.warn("Can't find level contains node [" + nodeKey.toString() + "] for update.");
                return false;
            } else {
                patternNodes.put(nodeKey.getLevelKey(), nodesFromCenter);
            }
        }

        assert patternNodes.containsKey(nodeKey.getLevelKey()) : "Something wrong in getNodesFromCenter";

        if (!patternNodes.get(nodeKey.getLevelKey()).containsKey(nodeKey)) {
            logger.warn("Can't find node [" + nodeKey.toString() + "] for update.");
            return false;
        }

        if(setNodeToCenter(nodeKey, node)) {
            if (patternNodes.containsKey(nodeKey.getLevelKey())) {
                patternNodes.get(nodeKey.getLevelKey()).put(nodeKey, node);
            } else {
                Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
                nodes.put(nodeKey, node);
                patternNodes.put(nodeKey.getLevelKey(), nodes);
            }
            return true;
        }
        return false;
    }

    private Map<PatternNodeKey, PatternNode> getNodes(PatternLevelKey levelKey) {
        //For java pass object by reference
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        //TODO: get from Redis
        if (!patternNodes.containsKey(levelKey)) {
            Map<PatternNodeKey, PatternNode> nodesFromCenter = getNodesFromCenter(levelKey);
            if (nodesFromCenter != null) {
                patternNodes.put(levelKey, nodesFromCenter);
                nodes.putAll(nodesFromCenter);
            }
        } else {
            nodes.putAll(patternNodes.get(levelKey));
        }

        return nodes;
    }

    //TODO:
    private Map<PatternNodeKey, PatternNode> getNodesFromCenter(PatternLevelKey levelKey) {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        return nodes;
    }

    private PatternNode getNode(PatternNodeKey nodeKey) {
        if (!patternNodes.containsKey(nodeKey.getLevelKey())) {
            Map<PatternNodeKey, PatternNode> nodesFromCenter = getNodesFromCenter(nodeKey.getLevelKey());
            if (nodesFromCenter != null) {
                patternNodes.put(nodeKey.getLevelKey(), nodesFromCenter);
            }
        }
        return patternNodes.containsKey(nodeKey.getLevelKey()) && patternNodes.get(nodeKey.getLevelKey()).containsKey(nodeKey) ?
                patternNodes.get(nodeKey.getLevelKey()).get(nodeKey) : null;
    }

    //TODO:
    private boolean setNodeToCenter(PatternNodeKey nodeKey, PatternNode node) {
       return true;
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
}
