package com.company.platform.team.projpatternreco.stormtopology.refinder;

import com.company.platform.team.projpatternreco.common.data.*;
import com.company.platform.team.projpatternreco.common.modules.FastClustering;
import com.company.platform.team.projpatternreco.stormtopology.utils.PatternRecognizeException;
import com.company.platform.team.projpatternreco.stormtopology.utils.RedisNodeCenter;
import com.company.platform.team.projpatternreco.stormtopology.leaffinder.PatternLeaves;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
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

    private ConcurrentHashMap<PatternLevelKey, ConcurrentHashMap<PatternNodeKey, PatternNode>> patternNodes;
    private Map confMap;
    private static PatternNodes forest;

    public static synchronized PatternNodes getInstance(Map conf) {
        if (forest == null) {
            forest = new PatternNodes(conf);
        }
        return forest;
    }

    private PatternNodes(Map conf) {
        try {
            patternNodes = new ConcurrentHashMap<>();
            this.confMap = conf;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public PatternNodeKey getParentNodeId(List<String> tokens,
                                          PatternLevelKey levelKey,
                                          double maxDistance,
                                          int retryTolerence) throws InvalidParameterException{
        if (tokens == null || levelKey == null) {
            logger.error("invalid parameters. tokens or levelKey is null.");
            throw new InvalidParameterException("invalid parameters, tokens or levelKey is null.");
        }

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

        logger.debug("Already retried " + retryTolerence + " times, still can't find parent id for given tokens.");
        return null;
    }

    public Pair<PatternNodeKey, List<String>> mergePatternToNode(PatternNodeKey key,
                                                                 List<String> patternTokens,
                                                                 double maxDist,
                                                                 boolean isLastLevel)
            throws PatternRecognizeException, InvalidParameterException{
        if (key == null || patternTokens == null) {
            logger.error("invalid parameter, key or patternTokens is null");
            throw new InvalidParameterException("key or patternTokens is null");
        }

        PatternNode parentNode = getNode(key);
        if (parentNode == null) {
            logger.error("can not find node for key: " + key);
            throw new PatternRecognizeException("can not find node for key: " + key);
        }

        try {
            List<String> mergedTokens = Aligner.retrievePattern(parentNode.getPatternTokens(), patternTokens);
            PatternLevelKey levelKey = new PatternLevelKey(key.getProjectName(), key.getLevel()+1);
            boolean grandNodeAdded = false;
            if (isLastLevel) {
                logger.debug("this is the last level of pattern, only merge, will not add parent node for node: " + key.toString());
            }
            if (!parentNode.hasParent() && !isLastLevel) {
                PatternNodeKey grandNodeKey = getParentNodeId(mergedTokens, levelKey, maxDist, 1);
                if (grandNodeKey == null) {
                    grandNodeKey = new PatternNodeKey(levelKey);
                    addNode(grandNodeKey, new PatternNode(mergedTokens));
                }
                parentNode.setParent(grandNodeKey);
                grandNodeAdded = true;
            }

            boolean parentNodeUpdated = false;
            if (!mergedTokens.equals(parentNode.getPatternTokens())) {
                parentNode.updatePatternTokens(mergedTokens);
                updateNode(key, parentNode);
                parentNodeUpdated = true;
            }

            if (grandNodeAdded || parentNodeUpdated) {
                return Pair.of(parentNode.getParentId(), mergedTokens);
            }
        } catch (Exception e) {
            logger.warn("Failed to merge tokens to it's parent pattern.", e);
            throw new PatternRecognizeException("Failed to merge tokens to it's parent pattern" + e.getMessage());
        }

        logger.debug("No need to update node [" + key.toString() + "] pattern.");
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
        if(RedisNodeCenter.getInstance(confMap).addNode(nodeKey, node)) {
            if (patternNodes.containsKey(nodeKey.getLevelKey())) {
                patternNodes.get(nodeKey.getLevelKey()).put(nodeKey, node);
            } else {
                ConcurrentHashMap<PatternNodeKey, PatternNode> nodes = new ConcurrentHashMap<>();
                nodes.put(nodeKey, node);
                patternNodes.put(nodeKey.getLevelKey(), nodes);
            }
            logger.debug("add node to PatternNodes, key:" + nodeKey.getLevelKey() + ", " + node.toString());
            return true;
        }
        return false;
    }

    private boolean updateNode(PatternNodeKey nodeKey, PatternNode node) {
        if (!patternNodes.containsKey(nodeKey.getLevelKey())) {
            ConcurrentHashMap<PatternNodeKey, PatternNode> nodesFromCenter =
                    new ConcurrentHashMap<>(RedisNodeCenter.getInstance(confMap).getProjectLevelNodes(nodeKey.getLevelKey()));
            if (nodesFromCenter == null) {
                logger.info("Can't find level contains node [" + nodeKey.toString() + "] for update.");
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

        if(RedisNodeCenter.getInstance(confMap).updateNode(nodeKey, node)) {
            if (patternNodes.containsKey(nodeKey.getLevelKey())) {
                patternNodes.get(nodeKey.getLevelKey()).put(nodeKey, node);
            } else {
                ConcurrentHashMap<PatternNodeKey, PatternNode> nodes = new ConcurrentHashMap<>();
                nodes.put(nodeKey, node);
                patternNodes.put(nodeKey.getLevelKey(), nodes);
            }
            logger.debug("update node key:" + nodeKey.getLevelKey() + ", " + node.toString());
            return true;
        }
        return false;
    }

    private Map<PatternNodeKey, PatternNode> getNodes(PatternLevelKey levelKey) {
        //For java pass object by reference
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        //TODO: get from Redis
        if (!patternNodes.containsKey(levelKey)) {
            ConcurrentHashMap<PatternNodeKey, PatternNode> nodesFromCenter
                    = new ConcurrentHashMap<>(RedisNodeCenter.getInstance(confMap).getProjectLevelNodes(levelKey));
            if (nodesFromCenter != null) {
                logger.debug("get from center");
                patternNodes.put(levelKey, nodesFromCenter);
                nodes.putAll(nodesFromCenter);
            }
        } else {
            nodes.putAll(patternNodes.get(levelKey));
        }

        return nodes;
    }

    private Map<PatternNodeKey, PatternNode> getNodesFromLeaves(PatternLevelKey levelKey) {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        nodes.putAll(PatternLeaves.getInstance(confMap).getNodes(levelKey));
        logger.debug("nodes from leaves: " + nodes.keySet().toString());
        return nodes;
    }

    //private PatternNode getNode(PatternNodeKey nodeKey) {
    public PatternNode getNode(PatternNodeKey nodeKey) {
        if (!patternNodes.containsKey(nodeKey.getLevelKey())
                || !patternNodes.get(nodeKey.getLevelKey()).containsKey(nodeKey)) {
            ConcurrentHashMap<PatternNodeKey, PatternNode> nodesFromCenter =
                    new ConcurrentHashMap<>(RedisNodeCenter.getInstance(confMap).getProjectLevelNodes(nodeKey.getLevelKey()));
            if (nodesFromCenter != null) {
                patternNodes.put(nodeKey.getLevelKey(), nodesFromCenter);
            } else {
                logger.warn("can not find level nodes from center, level key: " + nodeKey.getLevelKey().toString());
            }
        }

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
            if (nodes == null) {
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
