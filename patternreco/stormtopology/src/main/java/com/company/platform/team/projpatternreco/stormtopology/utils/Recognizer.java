package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.modules.FastClustering;
import com.company.platform.team.projpatternreco.common.preprocess.Preprocessor;
import com.company.platform.team.projpatternreco.stormtopology.data.*;
import com.company.platform.team.projpatternreco.stormtopology.eventbus.*;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by admin on 2018/8/2.
 */
public final class Recognizer implements Serializable{
    private static final Logger logger = LoggerFactory.getLogger(Recognizer.class);

    private PatternNodes localPatternNodes;
    private PatternMetas localPatternMetas;
    private RedisUtil nodeCenter;
    private MysqlUtil mysqlUtil;

    private static Recognizer instance = null;

    public static synchronized Recognizer getInstance(Map conf) {
        if (instance == null) {
            instance = new Recognizer(conf);
        }
        return instance;
    }

    private Recognizer(Map conf) {
        localPatternNodes = new PatternNodes();

        try {
            Map redisConf = (Map) conf.get(Constants.CONFIGURE_REDIS_SECTION);
            nodeCenter = RedisUtil.getInstance(redisConf);
        } catch (Exception e) {
            logger.warn("there is no node center, all the nodes will be cached local.");
        }

        try {
            Map mysqlConf = (Map) conf.get(Constants.CONFIGURE_MYSQL_SECTION);
            mysqlUtil = MysqlUtil.getInstance(mysqlConf);
        } catch (Exception e) {
            logger.warn("get mysql instance failed.");
        }

        Map patternrecoConf = (Map) conf.get(Constants.CONFIGURE_PATTERNRECO_SECTION);
        localPatternMetas = PatternMetas.getInstance(patternrecoConf, nodeCenter);
        EventBus eventBus = EventBus.getInstance();
        if (localPatternNodes != null) {
            eventBus.registerListener(localPatternNodes);
        }
    }

    //do it before submit the topology
    public void initMetas() {
        //synchronize metas from mysql to redis
        if (mysqlUtil != null && nodeCenter != null) {
            List<DBProject> projects = mysqlUtil.getProjectMetas();
            if (projects != null) {
                for (DBProject project : projects) {
                    String projectName = project.getProjectName();
                    nodeCenter.setMetaData(projectName,
                            PatternMetaType.PROJECT_ID.getTypeString(), String.valueOf(project.getId()));
                    nodeCenter.setMetaData(projectName, PatternMetaType.LEAF_NODES_LIMIT.getTypeString(),
                            String.valueOf(project.getLeafMax()));
                }
            }
        }
    }

    public Pair<PatternNodeKey, List<String>> getLeafNodeId(String projectName, String log) {
        //preprocess
        String logBody = log;
        int bodyLengthMax = localPatternMetas.getBodyLengthMax();
        if (log.length() > bodyLengthMax) {
            logger.info("log body is too long, the max length we can handle is " + bodyLengthMax + ",will eliminate the exceeded");
            logBody = log.substring(0, bodyLengthMax);
        }
        List<String> bodyTokens = Preprocessor.transform(logBody);

        int tokenCountMax = localPatternMetas.getTokenCountMax();
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
    public PatternNodeKey getParentNodeId(PatternLevelKey levelKey, List<String> tokens ) {
        assert tokens!=null && levelKey !=null : "tokens or levelKey is null.";

        //if no nodes, get from nodeCenter
        if (localPatternNodes.getLevelNodeSize(levelKey) == 0) {
            synchronizeNodesFromCenter(levelKey);
        }

        Map<PatternNodeKey, PatternNode> levelNodes = new HashMap<>();
        //if still no nodes, return null
        if (localPatternNodes.getLevelNodeSize(levelKey) > 0) {
            levelNodes.putAll(localPatternNodes.getNodes(levelKey));
        } else {
            return null;
        }

        double similarity = localPatternMetas.getProjectSimilarity(levelKey);
        PatternNodeKey nodeKey = findNodeIdFromNodes(tokens, levelNodes, 1-similarity);
        if (nodeKey != null) {
            return nodeKey;
        }

        int retryTolerence = localPatternMetas.getFindTolerence();
        int triedTimes = 0;
        do {
            synchronizeNodesFromCenter(levelKey);
            Map<PatternNodeKey, PatternNode> newLevelNodes = localPatternNodes.getNodes(levelKey);
            if (newLevelNodes == null || newLevelNodes.size() == 0) {
                triedTimes++;
                continue;
            }
            Map<PatternNodeKey, PatternNode> newAddedNodes = Maps.difference(levelNodes, newLevelNodes).entriesOnlyOnRight();
            nodeKey = findNodeIdFromNodes(tokens, newLevelNodes, 1-similarity);
            if (nodeKey != null) {
                return nodeKey;
            }
            levelNodes.putAll(newAddedNodes);
            triedTimes++;
        } while (triedTimes < retryTolerence);

        logger.debug("Already retried " + retryTolerence + " times, still can't find parent id for given tokens.");
        return null;
    }
    private PatternNodeKey findNodeIdFromNodes(List<String> tokens,
                                               Map<PatternNodeKey, PatternNode> levelNodes,
                                               double maxDistance) {
        if (levelNodes != null) {
            for (Map.Entry<PatternNodeKey, PatternNode> node: levelNodes.entrySet()) {
                if (FastClustering.belongsToCluster(tokens, node.getValue().getRepresentTokens(), maxDistance)) {
                    logger.debug("found parent Node for tokens: " + tokens + ", parent tokens: " + node.getValue().getRepresentTokens()
                            + ", maxDistance: " + maxDistance);
                    return node.getKey();
                }
            }
        }
        return null;
    }

    public void mergeTokenToNode(PatternNodeKey nodeKey, List<String> tokens)
            throws PatternRecognizeException{

        PatternNodeKey currentNodeKey = nodeKey;
        List<String> currentTokens = tokens;
        //merge i-1, add i
        int levelMax = localPatternMetas.getPatternLevelMax();
        for (int i = nodeKey.getLevel(); i < levelMax + 1; i++) {
            boolean isLastLevel = (i == levelMax) ? true : false;
            Pair<PatternNodeKey, List<String>> nextLevelTuple = updateNodeWithTokens(currentNodeKey, currentTokens, isLastLevel);
            if (nextLevelTuple == null) {
                break;
            }
            currentNodeKey = nextLevelTuple.getLeft();
            currentTokens = nextLevelTuple.getRight();
        }
    }
    private Pair<PatternNodeKey, List<String>> updateNodeWithTokens(PatternNodeKey key,
                                                                 List<String> patternTokens,
                                                                 boolean isLastLevel)
            throws PatternRecognizeException{

        assert key!=null && patternTokens!=null: "key or patternTokens is null";

        PatternNode parentNode = localPatternNodes.getNode(key);
        if (parentNode == null) {
            parentNode = nodeCenter.getNode(key);
        }

        if (parentNode == null) {
                logger.error("can not find node for key: " + key.toString());
                return null;
        }

        try {
            List<String> mergedTokens = Aligner.retrievePattern(parentNode.getPatternTokens(), patternTokens);
            PatternLevelKey levelKey = new PatternLevelKey(key.getProjectName(), key.getLevel()+1);
            boolean grandNodeAdded = false;
            if (!parentNode.hasParent() && !isLastLevel) {
                List<String> representTokens = parentNode.getRepresentTokens();
                PatternNodeKey grandNodeKey = getParentNodeId(levelKey, representTokens);
                if (grandNodeKey == null) {
                    PatternNode grandNode = new PatternNode(representTokens);
                    grandNode.updatePatternTokens(parentNode.getPatternTokens());
                    grandNodeKey = addNode(levelKey, grandNode);
                }
                if (grandNodeKey != null) {
                    parentNode.setParent(grandNodeKey);
                    grandNodeAdded = true;
                } else {
                    logger.error("set parent for node: " + key.toString() + "failed.");
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
            throw new PatternRecognizeException("Failed to merge tokens to it's parent pattern." + e.getMessage());
        }

        logger.debug("No need to update node [" + key.toString() + "] pattern.");
        return null;
    }

    public PatternNodeKey addNode(PatternLevelKey levelKey, PatternNode node) {
        synchronizeNodesFromCenter(levelKey);
        PatternNodeKey nodeKey = localPatternNodes.getNodeKeyByRepresentTokens(levelKey, node.getRepresentTokens());
        if (nodeKey == null) {
            nodeKey = nodeCenter.addNode(levelKey, node);
            if (nodeKey != null) {
                localPatternNodes.addNode(nodeKey, node);
            }
        }
        return nodeKey;
    }

    public void limitLeafCapacity(String projectName) {
        int leafLimit = localPatternMetas.getProjectLeafCountMax(projectName);
        //can not use local, because can not make it's in same worker as project's appender;
        long leafNodeSize = nodeCenter.getLevelNodeSize(new PatternLevelKey(projectName, 0));
        if (leafNodeSize > leafLimit && localPatternMetas.stepDownProjectLeafSimilarity(projectName)) {
            //delete local and nodeCenter
            localPatternNodes.deleteProjectNodes(projectName);
            nodeCenter.deleteProjectNodes(projectName);
            localPatternMetas.setPatternNew(projectName, true);
        }
    }
    public boolean flushNodesFromRedisToMysql(String projectName) {
        boolean success = false;
        if (!StringUtils.isEmpty(projectName)) {
            Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
            //only flush from level 1, level 0 will be processed in relink
            for(int i=1; i<localPatternMetas.getPatternLevelMax()+1; i++) {
                Map<PatternNodeKey, PatternNode> levelNodes = nodeCenter.getLevelNewNodes(null, new PatternLevelKey(projectName, i));
                if (levelNodes != null && levelNodes.size() > 0) {
                    nodes.putAll(levelNodes);
                }
            }
            int projectId = localPatternMetas.getProjectId(projectName);
            if (projectId > 0) {
                List<DBProjectPatternNode> DBNodes = CommonUtil.formatDBPatternNodes(nodes, projectId);
                if (DBNodes != null && DBNodes.size() > 0) {
                    mysqlUtil.refreshProjectNodes(projectId, DBNodes);
                }
                success = true;
            } else {
                logger.error("invalid projectId " + projectName + " for project " + projectName);
            }
        }
        return success;
    }
    public void relinkProjectLeaves(String projectName) {
        if (!StringUtils.isEmpty(projectName)) {
            List<DBProjectPatternNode> dbNodes = mysqlUtil.getProjectLeaves(projectName);
            if (dbNodes != null) {
                int projectId = localPatternMetas.getProjectId(projectName);
                if (projectId > 0) {
                    Map<PatternNodeKey, PatternNode> patternNodes = CommonUtil.formatPatternNode(dbNodes, projectName);
                    Map<PatternNodeKey, PatternNode> redisPatternNodes = nodeCenter.getLevelNewNodes(null,
                            new PatternLevelKey(projectName, 0));
                    if (redisPatternNodes == null || redisPatternNodes.size() == 0) {
                        return;
                    }

                    //insert new Nodes
                    MapDifference<PatternNodeKey, PatternNode> nodeMapDifference = Maps.difference(patternNodes, redisPatternNodes);
                    List<DBProjectPatternNode> insertNodes = CommonUtil.formatDBPatternNodes(nodeMapDifference.entriesOnlyOnRight(), projectId);
                    if (insertNodes == null || insertNodes.size() == 0) {
                        return;
                    }
                    if (mysqlUtil.insertNodes(insertNodes) > 0) {
                        logger.info("insert new leaves to mysql success.");
                    } else {
                        logger.error("insert new leaves to mysql failed.");
                    }

                    //if this is a new tree, relink level 0, and then set it to old tree.
                    if (localPatternMetas.isPatternNew(projectName)) {
                        for(Map.Entry<PatternNodeKey, PatternNode> entry : nodeMapDifference.entriesOnlyOnLeft().entrySet()) {
                            PatternNodeKey newParentKey = getParentNodeId(entry.getKey().getLevelKey(), entry.getValue().getRepresentTokens());
                            if (newParentKey != null) {
                                DBProjectPatternNode dbNode = new DBProjectPatternNode(projectId,
                                        entry.getKey().getLevel(),
                                        entry.getKey().getId());
                                dbNode.setParentKey(newParentKey.getId());
                                mysqlUtil.updateParentNode(dbNode);
                            }
                        }
                        localPatternMetas.setPatternNew(projectName, false);
                    }
                }
            }
        }
    }

    private void updateNode(PatternNodeKey nodeKey, PatternNode node) {
        if (nodeCenter.updateNode(nodeKey, node)) {
            localPatternNodes.updateNode(nodeKey, node);
        } else {
            logger.error("update node: " + nodeKey + "failed.");
        }
    }
    private void synchronizeNodesFromCenter(PatternLevelKey levelKey) {
        if (nodeCenter == null) {
            logger.info("no node center.");
            return;
        }
        Set<PatternNodeKey> localNodeKeys = localPatternNodes.getNodeKeys(levelKey);
        Map<PatternNodeKey, PatternNode> newNodesFromCenter = nodeCenter.getLevelNewNodes(localNodeKeys, levelKey);
        if (newNodesFromCenter != null && newNodesFromCenter.size() > 0) {
            logger.info("get " + newNodesFromCenter.size() + " new nodes from center for key: " + levelKey.toString());
            localPatternNodes.addNodes(newNodesFromCenter);
        } else {
            logger.info("no new nodes from center for key: " + levelKey.toString());
        }
    }
}
