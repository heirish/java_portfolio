package com.company.platform.team.projpatternreco.stormtopology.refinder;

import com.company.platform.team.projpatternreco.common.data.*;
import com.company.platform.team.projpatternreco.common.modules.FastClustering;
import com.company.platform.team.projpatternreco.stormtopology.utils.PatternRecognizeException;
import com.company.platform.team.projpatternreco.stormtopology.utils.RedisNodeCenter;
import com.google.common.collect.Maps;
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
    private static final Logger logger = LoggerFactory.getLogger(PatternNodes.class);

    private ConcurrentHashMap<PatternLevelKey, ConcurrentHashMap<PatternNodeKey, PatternNode>> patternNodes;
    private RedisNodeCenter nodeCenter;
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
            nodeCenter = RedisNodeCenter.getInstance(conf);
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
        if (levelNodes == null || levelNodes.size() == 0) {
            return null;
        }

        PatternNodeKey nodeKey = findNodeIdFromNodes(tokens, levelNodes, maxDistance);
        if (nodeKey != null) {
            return nodeKey;
        }

        int triedTimes = 0;
        do {
            Map<PatternNodeKey, PatternNode> newlevelNodes = getNewNodes(levelKey);
            if (newlevelNodes == null || newlevelNodes.size() == 0) {
                triedTimes++;
                continue;
            }
            nodeKey = findNodeIdFromNodes(tokens, newlevelNodes, maxDistance);
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
            throw new InvalidParameterException("can not find node for key: " + key + ",may be the key is invalid");
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
                    grandNodeKey = addNode(levelKey, new PatternNode(mergedTokens));
                    if (grandNodeKey != null) {
                        parentNode.setParent(grandNodeKey);
                        grandNodeAdded = true;
                    }
                }
            }

            boolean parentNodeUpdated = false;
            if (grandNodeAdded || !mergedTokens.equals(parentNode.getPatternTokens())) {
                parentNode.updatePatternTokens(mergedTokens);
                updateNode(key, parentNode);
                parentNodeUpdated = true;
            }

            if (parentNodeUpdated && parentNode.hasParent()) {
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

    public PatternNodeKey addNode(PatternLevelKey levelKey, PatternNode node ) {
        PatternNodeKey nodeKey = getNodeByRepresentTokens(levelKey, node.getRepresentTokens());
        if (nodeKey == null) {
            nodeKey = nodeCenter.addNode(levelKey, node);
            if(nodeKey != null) {
                if (patternNodes.containsKey(nodeKey.getLevelKey())) {
                    patternNodes.get(nodeKey.getLevelKey()).put(nodeKey, node);
                } else {
                    ConcurrentHashMap<PatternNodeKey, PatternNode> nodes = new ConcurrentHashMap<>();
                    nodes.put(nodeKey, node);
                    patternNodes.put(nodeKey.getLevelKey(), nodes);
                }
                logger.debug("add node to PatternNodes, key:" + nodeKey.getLevelKey() + ", " + node.toString());
            } else {
                logger.warn("add node to PatternNodes failed, key:" + nodeKey.getLevelKey() + ", " + node.toString());
            }
        }
        return nodeKey;
    }

    private boolean updateNode(PatternNodeKey nodeKey, PatternNode node) {
        if (!patternNodes.containsKey(nodeKey.getLevelKey())) {
            getNewNodes(nodeKey.getLevelKey());
        }

        assert patternNodes.containsKey(nodeKey.getLevelKey()) : "Something wrong in getNodesFromCenter";

        if (!patternNodes.get(nodeKey.getLevelKey()).containsKey(nodeKey)) {
            logger.warn("Can't find node [" + nodeKey.toString() + "] for update.");
            return false;
        }

        if(nodeCenter.updateNode(nodeKey, node)) {
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

    public PatternNodeKey getNodeByRepresentTokens(PatternLevelKey levelKey, List<String> representTokens) {
        if (patternNodes.containsKey(levelKey)) {
            for (Map.Entry<PatternNodeKey, PatternNode> node : patternNodes.get(levelKey).entrySet()) {
                if (node.getValue().getRepresentTokens().equals(representTokens)) {
                    return node.getKey();
                }
            }
        }
        return null;
    }

    public Map<PatternNodeKey, PatternNode> getNodes(PatternLevelKey levelKey) {
        if (!patternNodes.containsKey(levelKey)) {
            getNewNodes(levelKey);
        }

        if (patternNodes.containsKey(levelKey)) {
            return patternNodes.get(levelKey);
        }

        return null;
    }

    private Map<PatternNodeKey, PatternNode> getNewNodes(PatternLevelKey levelKey) {
        Set<PatternNodeKey> localNodeKeys = null;
        if (patternNodes.containsKey(levelKey)) {
            localNodeKeys = patternNodes.get(levelKey).keySet();
        }
        Map<PatternNodeKey, PatternNode> newNodesFromCenter = nodeCenter.getLevelNewNodes(localNodeKeys, levelKey);
        if (newNodesFromCenter != null && newNodesFromCenter.size() > 0) {
            if (patternNodes.containsKey(levelKey)) {
                patternNodes.get(levelKey).putAll(newNodesFromCenter);
            } else {
                patternNodes.put(levelKey, new ConcurrentHashMap<>(newNodesFromCenter));
            }
        } else {
            logger.info("no new nodes from center for key: " + levelKey.toString());
        }
        return newNodesFromCenter;
    }

    //private PatternNode getNode(PatternNodeKey nodeKey) {
    public PatternNode getNode(PatternNodeKey nodeKey) {
        if (!patternNodes.containsKey(nodeKey.getLevelKey())
                || !patternNodes.get(nodeKey.getLevelKey()).containsKey(nodeKey)) {
            getNewNodes(nodeKey.getLevelKey());
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

    //test
    public void deleteProjectNodes(String projectName) {
        for(int i=0; i< 11; i++) {
            PatternLevelKey levelKey = new PatternLevelKey(projectName, i);
            if (patternNodes.containsKey(levelKey)) {
                patternNodes.remove(levelKey);
            }

            if (patternNodes.containsKey(levelKey)) {
                logger.info("delete levelKey " + levelKey.toString() + "failed.");
            } else {
                logger.info("delete levelKey " + levelKey.toString() + "success.");
            }
        }
    }
}
