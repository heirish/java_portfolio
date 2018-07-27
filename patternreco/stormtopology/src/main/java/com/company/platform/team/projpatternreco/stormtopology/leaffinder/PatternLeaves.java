package com.company.platform.team.projpatternreco.stormtopology.leaffinder;

import com.company.platform.team.projpatternreco.common.data.*;
import com.company.platform.team.projpatternreco.common.modules.FastClustering;
import com.company.platform.team.projpatternreco.stormtopology.refinder.PatternNodes;
import com.company.platform.team.projpatternreco.stormtopology.utils.Constants;
import com.company.platform.team.projpatternreco.stormtopology.utils.RedisNodeCenter;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by admin on 2018/6/29.
 * Singletone
 */
// TODO:thread safe
public final class PatternLeaves {
    private static final Logger logger = LoggerFactory.getLogger(PatternLeaves.class);
    private static final Gson gson = new Gson();

    private ConcurrentHashMap<PatternLevelKey, ConcurrentHashMap<PatternNodeKey, PatternNode>> patternLeaves;
    private RedisNodeCenter nodeCenter;
    private static PatternLeaves leaves;

    public static synchronized PatternLeaves getInstance(Map conf) {
        if (leaves == null) {
            leaves = new PatternLeaves(conf);
        }
        return leaves;
    }

    private PatternLeaves(Map conf) {
        nodeCenter = RedisNodeCenter.getInstance(conf);
        this.patternLeaves = new ConcurrentHashMap<>();
        try {
            Map<PatternNodeKey, PatternNode> nodes = readFromFile("tree/patternLeaves");
            //split nodes by LevelKey
            SortedSet<PatternNodeKey> keys = new TreeSet<>(nodes.keySet());
            ConcurrentHashMap<PatternNodeKey, PatternNode> projectLeaves = new ConcurrentHashMap<>();
            PatternLevelKey lastLevelKey= null;
            for (PatternNodeKey key : keys) {
                if (!key.getProjectName().equals(lastLevelKey) && lastLevelKey != null) {
                    patternLeaves.put(lastLevelKey, projectLeaves);
                    lastLevelKey = key.getLevelKey();
                    projectLeaves = new ConcurrentHashMap<>();
                }
                projectLeaves.put(key, nodes.get(key));
            }
            //The last
            if (keys.size() > 0 && lastLevelKey != null && projectLeaves.size() > 0) {
                patternLeaves.put(keys.last().getLevelKey(), projectLeaves);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public PatternNodeKey getParentNodeId(List<String> tokens,
                                          PatternLevelKey levelKey,
                                          double maxDistance,
                                          int retryTolerence ){
        Map<PatternNodeKey, PatternNode> levelNodes = getNodes(levelKey);
        if (levelNodes != null) {
            PatternNodeKey nodeKey = findNodeIdFromNodes(tokens, levelNodes, maxDistance);
            if (nodeKey != null) {
                return nodeKey;
            }
        }

        int triedTimes = 0;
        do {
            Map<PatternNodeKey, PatternNode> newLevelNodes = getNewNodes(levelKey);
            if (newLevelNodes == null || newLevelNodes.size() == 0) {
                break;
            }
            PatternNodeKey nodeKey = findNodeIdFromNodes(tokens, newLevelNodes, maxDistance);
            if (nodeKey != null) {
                return nodeKey;
            }
            triedTimes++;
        } while (triedTimes < retryTolerence);

        return null;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<PatternLevelKey, ConcurrentHashMap<PatternNodeKey, PatternNode>> entry :patternLeaves.entrySet()) {
            for (Map.Entry<PatternNodeKey, PatternNode> entryNode : entry.getValue().entrySet()) {
                stringBuilder.append(entryNode.getKey().toString() + " : ");
                stringBuilder.append(entryNode.getValue().toJson());
                stringBuilder.append(System.getProperty("line.separator"));
            }
        }
        return stringBuilder.toString();
    }

    private Map<PatternNodeKey, PatternNode> readFromFile(String fileName) throws Exception {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    Map<String, String> fields = gson.fromJson(line, Constants.LOG_MAP_TYPE);
                    PatternNodeKey key = PatternNodeKey.fromString(fields.get(Constants.FIELD_PATTERNID));
                    List<String> patternTokens = Arrays.asList(fields.get(Constants.FIELD_PATTERNTOKENS)
                            .split(Constants.PATTERN_TOKENS_DELIMITER));
                    List<String> representTokens = Arrays.asList(fields.get(Constants.FIELD_REPRESENTTOKENS)
                            .split(Constants.PATTERN_TOKENS_DELIMITER));
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

    public PatternNodeKey getNodeByRepresentTokens(PatternLevelKey levelKey, List<String> representTokens) {
        if (patternLeaves.containsKey(levelKey)) {
            for (Map.Entry<PatternNodeKey, PatternNode> node : patternLeaves.get(levelKey).entrySet()) {
                if (node.getValue().getRepresentTokens().equals(representTokens)) {
                    return node.getKey();
                }
            }
        }
        return null;
    }

    public Map<PatternNodeKey, PatternNode> getNodes(PatternLevelKey levelKey) {
        if (!patternLeaves.containsKey(levelKey)) {
            getNewNodes(levelKey);
        }

        if (patternLeaves.containsKey(levelKey)) {
            return patternLeaves.get(levelKey);
        }

        return null;
    }

    private Map<PatternNodeKey, PatternNode> getNewNodes(PatternLevelKey levelKey) {
        Set<PatternNodeKey> localNodeKeys = null;
        if (patternLeaves.containsKey(levelKey)) {
            localNodeKeys = patternLeaves.get(levelKey).keySet();
        }
        Map<PatternNodeKey, PatternNode> newNodesFromCenter = nodeCenter.getLevelNewNodes(localNodeKeys, levelKey);
        if (newNodesFromCenter != null && newNodesFromCenter.size() > 0) {
            if (patternLeaves.containsKey(levelKey)) {
                patternLeaves.get(levelKey).putAll(newNodesFromCenter);
            } else {
                patternLeaves.put(levelKey, new ConcurrentHashMap<>(newNodesFromCenter));
            }
        } else {
            logger.info("no new nodes from center for key: " + levelKey.toString());
        }
        return newNodesFromCenter;
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

    public PatternNodeKey addNode(PatternLevelKey levelKey, PatternNode node) {
        //adjust the leaf similarity and update the redis
        if (patternLeaves.containsKey(levelKey) && patternLeaves.get(levelKey).size() > 500) {
            deleteProjectNodes(levelKey.getProjectName());
            PatternLeafSimilarity leafSimilarity = PatternLeafSimilarity.getInstance(-1);
            double oldSimilarity = leafSimilarity.getSimilarity(levelKey.getProjectName());
            double newSimilarity = leafSimilarity.decaySimilarity(levelKey.getProjectName());
            logger.info("project " + levelKey.getProjectName() + "'s leaf Similarity Changed from " + oldSimilarity + " to " + newSimilarity);
            nodeCenter.setProjectSimilarity(levelKey.getProjectName(), newSimilarity);
            return null;
        }

        PatternNodeKey nodeKey = getNodeByRepresentTokens(levelKey, node.getRepresentTokens());
        if (nodeKey == null) {
            nodeKey = nodeCenter.addNode(levelKey, node);
            if (nodeKey != null) {
                if (patternLeaves.containsKey(nodeKey.getLevelKey())) {
                    patternLeaves.get(nodeKey.getLevelKey()).put(nodeKey, node);
                } else {
                    ConcurrentHashMap<PatternNodeKey, PatternNode> nodes = new ConcurrentHashMap<>();
                    nodes.put(nodeKey, node);
                    patternLeaves.put(nodeKey.getLevelKey(), nodes);
                }
            }
        }
        return nodeKey;
    }

    //test
    private void deleteProjectNodes(String projectName) {
        nodeCenter.deleteProjectNodes(projectName);
        PatternNodes.getInstance(null).deleteProjectNodes(projectName);

        PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
        if (patternLeaves.containsKey(levelKey)) {
            patternLeaves.remove(new PatternLevelKey(projectName, 0));
        }
    }
}
