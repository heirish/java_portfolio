package com.company.platform.team.projpatternreco.stormtopology.leaffinder;

import com.company.platform.team.projpatternreco.common.data.*;
import com.company.platform.team.projpatternreco.common.modules.FastClustering;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;

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
    private ConcurrentHashMap<PatternLevelKey, ConcurrentHashMap<PatternNodeKey, PatternNode>> patternLeaves;

    private static PatternLeaves leaves;
    private static final Gson gson = new Gson();

    public static synchronized PatternLeaves getInstance() {
        if (leaves == null) {
            leaves = new PatternLeaves();
        }
        return leaves;
    }

    private PatternLeaves() {
        patternLeaves = new ConcurrentHashMap<>();
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

        return null;
    }

    private PatternNodeKey getLeafHasToken(PatternLevelKey levelKey, List<String> tokens) {
        if (patternLeaves != null && patternLeaves.containsKey(levelKey)) {
            Map<PatternNodeKey, PatternNode> levelNodes = patternLeaves.get(levelKey);
            for (Map.Entry<PatternNodeKey, PatternNode> entry : levelNodes.entrySet()) {
                if (entry.getValue().getRepresentTokens().equals(tokens)) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    public PatternNodeKey addNewLeaf(String projectName, List<String> tokens) {
       //first check if already exists
        PatternLevelKey levelKey = new PatternLevelKey(projectName, 0);
        PatternNodeKey nodeKey = getLeafHasToken(levelKey, tokens);
        if (nodeKey != null) {
            return nodeKey;
        }

        nodeKey = new PatternNodeKey(levelKey);
        PatternNode node = new PatternNode(tokens);
        //TODO:add to redis
        if (patternLeaves.containsKey(levelKey)) {
            patternLeaves.get(levelKey).put(nodeKey, node);
        } else {
            ConcurrentHashMap<PatternNodeKey, PatternNode> nodes = new ConcurrentHashMap<>();
            nodes.put(nodeKey, node);
            patternLeaves.put(levelKey, nodes);
        }
        return nodeKey;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<PatternLevelKey, ConcurrentHashMap<PatternNodeKey, PatternNode>> entry :patternLeaves.entrySet()) {
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

    private Map<PatternNodeKey, PatternNode> readFromFile(String fileName) throws Exception {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    Map<String, String> fields = gson.fromJson(line, Map.class);
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
    private Map<PatternNodeKey, PatternNode> getNodes(PatternLevelKey levelKey) {
        //For java pass object by reference
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        //TODO: get from Redis
        if (!patternLeaves.containsKey(levelKey)) {
            ConcurrentHashMap<PatternNodeKey, PatternNode> nodesFromCenter = new ConcurrentHashMap<>(getNodesFromCenter(levelKey));
            if (nodesFromCenter != null) {
                patternLeaves.put(levelKey, nodesFromCenter);
                nodes.putAll(nodesFromCenter);
            }
        } else {
            nodes.putAll(patternLeaves.get(levelKey));
        }

        return nodes;
    }

    private Map<PatternNodeKey, PatternNode> getNodesFromCenter(PatternLevelKey levelKey) {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        return nodes;
    }

    private long getMaxUpdatedTime(PatternLevelKey levelKey) {
        long maxUpdateTime = 0;
        for (Map.Entry<PatternLevelKey, ConcurrentHashMap<PatternNodeKey, PatternNode>> entry : patternLeaves.entrySet()) {
            for (Map.Entry<PatternNodeKey, PatternNode> nodeEntry : entry.getValue().entrySet()) {
                if (levelKey.equals(nodeEntry.getKey().getLevelKey())) {
                    maxUpdateTime = maxUpdateTime < nodeEntry.getValue().getLastupdatedTime() ?
                            nodeEntry.getValue().getLastupdatedTime() : maxUpdateTime;
                }
            }
        }
        return maxUpdateTime;
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

    private long addNode(PatternNodeKey nodeKey, PatternNode node,
                         long clientlastUpdatedTime) {
        //TODO: get maxUpdatedTime From DB
        long maxUpdateTime = getMaxUpdatedTime(nodeKey.getLevelKey());
        //client need to synchronize
        if (clientlastUpdatedTime < maxUpdateTime) {
            return 0;
        }

        node.setLastupdatedTime(System.currentTimeMillis());

        if (patternLeaves.containsKey(nodeKey.getLevelKey())) {
            patternLeaves.get(nodeKey.getLevelKey()).put(nodeKey, node);
        } else {
            ConcurrentHashMap<PatternNodeKey, PatternNode> nodes = new ConcurrentHashMap<>();
            nodes.put(nodeKey, node);
            patternLeaves.put(nodeKey.getLevelKey(), nodes);
        }
        return node.getLastupdatedTime();
    }
}
