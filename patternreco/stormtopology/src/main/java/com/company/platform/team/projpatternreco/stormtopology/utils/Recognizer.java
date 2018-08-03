package com.company.platform.team.projpatternreco.stormtopology.utils;

import clojure.lang.Cons;
import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.modules.FastClustering;
import com.company.platform.team.projpatternreco.common.preprocess.Preprocessor;
import com.company.platform.team.projpatternreco.stormtopology.eventbus.IEventListener;
import com.company.platform.team.projpatternreco.stormtopology.eventbus.IEventType;
import com.company.platform.team.projpatternreco.stormtopology.eventbus.MetaEvent;
import com.company.platform.team.projpatternreco.stormtopology.eventbus.SimilarityEvent;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by admin on 2018/8/2.
 */
//TODO: fresh meta and nodes should be together
//TODO: cache new nodes on local unsent to node center, and flush to center periodly
public final class Recognizer implements IEventListener{
    private static final Logger logger = LoggerFactory.getLogger(Recognizer.class);

    private PatternNodes localPatternNodes;
    private PatternMetas localPatternMetas;
    private ConcurrentHashMap<PatternLevelKey, Double> localProjectSimilarities;
    private RedisNodeCenter nodeCenter;

    private static Recognizer instance = null;

    public static synchronized Recognizer getInstance(Map conf) {
        if (instance == null) {
            instance = new Recognizer(conf);
        }
        return instance;
    }

    private Recognizer(Map conf) {
        localPatternNodes = new PatternNodes();
        localPatternMetas = PatternMetas.getInstance(conf);
        localProjectSimilarities = new ConcurrentHashMap<>();
        if (conf != null) {
            nodeCenter = RedisNodeCenter.getInstance(conf);
            logger.info("node center is enabled.");
        } else {
            logger.warn("there is no node center, all the nodes will be cached local.");
        }
    }

    public Pair<PatternNodeKey, List<String>> getLeafNodeId(String projectName, String log) {
        //preporcess
        String logBody = log;
        int bodyLengthMax = localPatternMetas.getBodyLengthMax(projectName);
        if (log.length() > bodyLengthMax) {
            logger.info("log body is too long, the max length we can handle is " + bodyLengthMax + ",will eliminate the exceeded");
            logBody = log.substring(0, bodyLengthMax);
        }
        List<String> bodyTokens = Preprocessor.transform(logBody);

        int tokenCountMax = localPatternMetas.getTokensCountMax(projectName);
        if (bodyTokens.size() > tokenCountMax) {
            logger.warn("sequenceLeft exceeds the max length we can handle, will eliminate the exceeded part to "
                    + Constants.IDENTIFY_EXCEEDED_TYPE + ",tokens:" + Arrays.toString(bodyTokens.toArray()));
            bodyTokens = new ArrayList(bodyTokens.subList(0, tokenCountMax - 1));
            bodyTokens.add(Constants.IDENTIFY_EXCEEDED_TYPE);
        }

        PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
        PatternNodeKey nodeKey = getParentNodeId(levelKey,bodyTokens);
        return Pair.of(nodeKey, bodyTokens);
    }

    public Pair<PatternNodeKey, Boolean> addLeafNode(String projectName, List<String> tokens) {
        PatternNodeKey nodeKey = null;
        PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);

        int leafLimit = localPatternMetas.getLeafNodesLimit(projectName);
        int leafNodeSize = localPatternNodes.getLevelNodeSize(levelKey);
        boolean similarityChanged = false;
        if (leafNodeSize > leafLimit && stepDownLeafSimilarity(projectName)) {
            //delete local and nodeCenter
            localPatternNodes.deleteLevelNodes(levelKey);
            nodeCenter.deleteLevelNodes(levelKey);

            nodeKey = getParentNodeId(levelKey, tokens);
            similarityChanged = true;
        }

        if (nodeKey == null) {
            PatternNode node = new PatternNode(tokens);
            nodeKey = addNode(levelKey, node);
        }
        return Pair.of(nodeKey, similarityChanged);
    }

    public void mergeTokenToNode(PatternNodeKey nodeKey, List<String> tokens)
            throws PatternRecognizeException, InvalidParameterException{
        String projectName = nodeKey.getProjectName();
        int levelMax = localPatternMetas.getPatternLevelMax(projectName);

        PatternNodeKey currentNodeKey = nodeKey;
        List<String> currentTokens = tokens;
        //merge i-1, add i
        for (int i = nodeKey.getLevel(); i < levelMax + 1; i++) {
            boolean isLastLevel = (i == levelMax) ? true : false;
            Pair<PatternNodeKey, List<String>> nextLevelTuple = updateNodeWithTokens(currentNodeKey, currentTokens,isLastLevel);
            if (nextLevelTuple == null) {
                break;
            }
            currentNodeKey = nextLevelTuple.getLeft();
            currentTokens = nextLevelTuple.getRight();
        }
    }

    public Pair<PatternNodeKey, List<String>> updateNodeWithTokens(PatternNodeKey key,
                                                                 List<String> patternTokens,
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
                PatternNodeKey grandNodeKey = getParentNodeId(levelKey, mergedTokens);
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

    //for test
    public PatternNodeKey getParentNodeId(PatternLevelKey levelKey, List<String> tokens )
            throws InvalidParameterException {
        if (tokens == null || levelKey == null) {
            logger.error("invalid parameters. tokens or levelKey is null.");
            throw new InvalidParameterException("invalid parameters, tokens or levelKey is null.");
        }
        Map<PatternNodeKey, PatternNode> levelNodes = localPatternNodes.getNodes(levelKey);
        if (levelNodes == null || levelNodes.size() == 0) {
            return null;
        }

        double similarity = getSimilarity(levelKey);
        PatternNodeKey nodeKey = findNodeIdFromNodes(tokens, levelNodes, 1-similarity);
        if (nodeKey != null) {
            return nodeKey;
        }

        int retryTolerence = localPatternMetas.getFindTolerance(levelKey.getProjectName());
        int triedTimes = 0;
        do {
            Map<PatternNodeKey, PatternNode> newlevelNodes = getNewNodesFromCenter(levelKey);
            if (newlevelNodes == null || newlevelNodes.size() == 0) {
                triedTimes++;
                continue;
            }
            nodeKey = findNodeIdFromNodes(tokens, newlevelNodes, 1-similarity);
            if (nodeKey != null) {
                return nodeKey;
            }
            triedTimes++;
        } while (triedTimes < retryTolerence);

        logger.debug("Already retried " + retryTolerence + " times, still can't find parent id for given tokens.");
        return null;
    }

    //for test
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

    private void updateNode(PatternNodeKey nodeKey, PatternNode node) {
        if (nodeCenter.updateNode(nodeKey, node)) {
            localPatternNodes.updateNode(nodeKey, node);
        }
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

    private Map<PatternNodeKey, PatternNode> getNewNodesFromCenter(PatternLevelKey levelKey) {
        Set<PatternNodeKey> localNodeKeys = localPatternNodes.getNodeKeys(levelKey);
        Map<PatternNodeKey, PatternNode> newNodesFromCenter = nodeCenter.getLevelNewNodes(localNodeKeys, levelKey);
        if (newNodesFromCenter != null && newNodesFromCenter.size() > 0) {
            logger.info("get " + newNodesFromCenter.size() + " new nodes from center for key: " + levelKey.toString());
        } else {
            logger.info("no new nodes from center for key: " + levelKey.toString());
        }
        return newNodesFromCenter;
    }

    private double getLeafSimilarity(String projectName) {
        PatternLevelKey leafLevelKey = new PatternLevelKey(projectName, 0);
        if (!localProjectSimilarities.containsKey(leafLevelKey)) {
            double leafSimilarite = localPatternMetas.getLeafSimilarityMax(projectName);
            localProjectSimilarities.put(leafLevelKey, leafSimilarite);
        }

        return localProjectSimilarities.get(projectName);
    }

    private double getSimilarity(PatternLevelKey levelKey) {
        String projectName = levelKey.getProjectName();
        if (!localProjectSimilarities.containsKey(levelKey)) {
            double leafSimilarity = getLeafSimilarity(projectName);
            double decayFactor = localPatternMetas.getSimilarityDecayFactor(projectName);
            double similarity =  leafSimilarity * Math.pow(1-decayFactor, levelKey.getLevel());
            localProjectSimilarities.put(levelKey, similarity);
        }
        return localProjectSimilarities.get(levelKey);
    }

    public boolean stepUpLeafSimilarity(String projectName) {
        double oldSimilarity = getLeafSimilarity(projectName);
        double highSimilarity = localPatternMetas.getLeafSimilarityMax(projectName);
        double newSimilarity =  (oldSimilarity + highSimilarity) /2;
        if (Math.abs(oldSimilarity - highSimilarity) < Constants.SIMILARITY_COMPARE_SPECIOUS) {
            return false;
        }
        PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
        localProjectSimilarities.put(levelKey, newSimilarity);

        String fingerPrint = UUID.randomUUID().toString();
        nodeCenter.setLeafSimilarity(projectName, String.valueOf(newSimilarity));
        return true;
    }

    public boolean stepDownLeafSimilarity(String projectName) {
        double oldSimilarity = getLeafSimilarity(projectName);
        double lowSimilarity = localPatternMetas.getLeafSimilarityMin(projectName);
        if (Math.abs(oldSimilarity - lowSimilarity) < Constants.SIMILARITY_COMPARE_SPECIOUS) {
            return false;
        }

        double newSimilarity =  (oldSimilarity + lowSimilarity) /2;
        PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
        localProjectSimilarities.put(levelKey, newSimilarity);
        String fingerPrint = UUID.randomUUID().toString();
        nodeCenter.setLeafSimilarity(projectName, String.valueOf(newSimilarity));
        return true;
    }

    @Override
    public void accept(IEventType event) {
        if (event instanceof SimilarityEvent) {
            String projectName = ((SimilarityEvent)event).getProjectName();
            if (refreshProjectSimilarity(projectName)) {
                deleteProjectNodes(projectName);
            }
        } else if (event instanceof MetaEvent) { // decay factor change
        }
    }

    private boolean refreshProjectSimilarity(String projectName) {
        try {
           double leafSimilarity = Double.parseDouble(nodeCenter.getLeafSimilarity(projectName));
           int levelMax = localPatternMetas.getPatternLevelMax(projectName);
           for (int i=0; i< levelMax + 1; i++) {
               PatternLevelKey levelKey = new PatternLevelKey(projectName, i);
               if (localProjectSimilarities.containsKey(levelKey)) {
                   localProjectSimilarities.remove(levelKey);
               }
           }
           localProjectSimilarities.put(new PatternLevelKey(projectName, 0), leafSimilarity);
           return true;
        } catch (Exception e) {
            logger.error("get new similarity from redis error, will not refresh local.");
        }
        return false;
    }

    private void deleteProjectNodes(String projectName) {
        int levelMax = localPatternMetas.getPatternLevelMax(projectName);
        for (int i=0; i< levelMax + 1; i++) {
            PatternLevelKey levelKey  = new PatternLevelKey(projectName, i);
            localPatternNodes.deleteLevelNodes(levelKey);
        }
    }
}
