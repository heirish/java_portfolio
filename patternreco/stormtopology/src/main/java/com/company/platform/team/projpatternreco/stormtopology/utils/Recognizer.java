package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.modules.FastClustering;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by admin on 2018/8/2.
 */
//TODO: fresh meta and nodes should be together
//TODO: cache new nodes on local unsent to node center, and flush to center periodly
public class Recognizer {
    private static final Logger logger = LoggerFactory.getLogger(Recognizer.class);
    private static final PatternNodes localPatternNodes = new PatternNodes();
    private static final PatternMetas localMetas = new PatternMetas();
    private static RedisNodeCenter nodeCenter = null;

    private static Recognizer instance = null;

    public static synchronized Recognizer getInstance(Map conf) {
        if (instance == null) {
            instance = new Recognizer(conf);
        }
        return instance;
    }

    private Recognizer(Map conf) {
        if (conf != null) {
            nodeCenter = RedisNodeCenter.getInstance(conf);
            logger.info("node center is enabled.");
        } else {
            logger.warn("there is no node center, all the nodes will be cached local.");
        }
    }

    public double getLeafSimilarityMin(String projectName) {
        double similarity;
        try {
        String leafSimilarity = nodeCenter.getMeta(projectName, PatternMetaType.LEAF_SIMILARITY_MIN.toString());
            similarity = Double.parseDouble(leafSimilarity);
        } catch (Exception e) {
            similarity = Constants.PATTERN_LEAF_SIMILARITY_MIN;
            logger.info("project " + projectName + " has no or invalid leaf similarity min meta data, will use default: " + similarity);
        }
        return similarity;
    }

    public PatternNodeKey getParentNodeId(List<String> tokens,
                                                 PatternLevelKey levelKey,
                                                 double maxDistance,
                                                 int retryTolerence) throws InvalidParameterException {
        if (tokens == null || levelKey == null) {
            logger.error("invalid parameters. tokens or levelKey is null.");
            throw new InvalidParameterException("invalid parameters, tokens or levelKey is null.");
        }
        Map<PatternNodeKey, PatternNode> levelNodes = localPatternNodes.getNodes(levelKey);
        if (levelNodes == null || levelNodes.size() == 0) {
            return null;
        }

        PatternNodeKey nodeKey = findNodeIdFromNodes(tokens, levelNodes, maxDistance);
        if (nodeKey != null) {
            return nodeKey;
        }

        int triedTimes = 0;
        do {
            Map<PatternNodeKey, PatternNode> newlevelNodes = getNewNodesFromCenter(levelKey);
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

    public boolean exceedLeafLimit(String projectName, int limit) {
        int nodeSize = localPatternNodes.getLevelNodeSize(new PatternLevelKey(projectName, 0));
        return nodeSize > limit;
    }

    public PatternNodeKey addNode(PatternLevelKey levelKey, PatternNode node) {
        PatternNodeKey nodeKey = localPatternNodes.getNodeKeyByRepresentTokens(levelKey, node.getRepresentTokens());
        if (nodeKey == null) {
            nodeKey = nodeCenter.addNode(levelKey, node);
            if (nodeKey != null) {
                localPatternNodes.addNode(nodeKey, node);
            }
        }
        return nodeKey;
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

        //TODO:find from recyled Leaves if nodeKeys's level is  0
        PatternNode parentNode = localPatternNodes.getNode(key);
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

    public void deleteLevelNodes(PatternLevelKey levelKey) {
       localPatternNodes.deleteLevelNodes(levelKey);
       nodeCenter.deleteLevelNodes(levelKey);
    }

    public void deleteLocalLevelNodes(PatternLevelKey levelKey) {
        localPatternNodes.deleteLevelNodes(levelKey);
    }

    private void updateNode(PatternNodeKey nodeKey, PatternNode node) {
        if (nodeCenter.updateNode(nodeKey, node)) {
            localPatternNodes.updateNode(nodeKey, node);
        }
    }

    private static PatternNodeKey findNodeIdFromNodes(List<String> tokens,
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

    private static Map<PatternNodeKey, PatternNode> getNewNodesFromCenter(PatternLevelKey levelKey) {
        Set<PatternNodeKey> localNodeKeys = localPatternNodes.getNodeKeys(levelKey);
        Map<PatternNodeKey, PatternNode> newNodesFromCenter = nodeCenter.getLevelNewNodes(localNodeKeys, levelKey);
        if (newNodesFromCenter != null && newNodesFromCenter.size() > 0) {
            logger.info("get " + newNodesFromCenter.size() + " new nodes from center for key: " + levelKey.toString());
        } else {
            logger.info("no new nodes from center for key: " + levelKey.toString());
        }
        return newNodesFromCenter;
    }
}