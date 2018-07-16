package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.*;
import com.company.platform.team.projpatternreco.modules.FastClustering;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.company.platform.team.projpatternreco.sparkandhadoop.patternerfiner.MapReduceRefinerWorker.retrievePattern;

/**
 * Created by admin on 2018/6/29.
 * Singletone
 */
// TODO:thread safe
public final class PatternNodes {
    //Map<project-level, Map<project-level-nodeid, PatternNode>>
    private ConcurrentHashMap<PatternNodeKey, PatternNode> patternNodes;
    private static PatternNodes forest;
    private static final Gson gson = new Gson();
    private ReadWriteLock lock;


    public static synchronized PatternNodes getInstance() {
        if (forest == null) {
            forest = new PatternNodes();
        }
        return forest;
    }

    private PatternNodes() {
        lock = new ReentrantReadWriteLock();
        //TODO:recover from local checkpoint
        try {
            patternNodes = new ConcurrentHashMap<>(readFromFile("tree/patternLeaves"));
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

    public PatternNodeKey getParentNodeId(List<String> tokens,
                                          PatternLevelKey levelKey,
                                          double maxDistance) {
        Map<PatternNodeKey, PatternNode> levelNodes = getNodes(levelKey);
        if (levelNodes != null) {
            for (Map.Entry<PatternNodeKey, PatternNode> node: levelNodes.entrySet()) {
                if (FastClustering.belongsToCluster(tokens, node.getValue().getRepresentTokens(), maxDistance)) {
                    return node.getKey();
                }
            }
        }

        int triedTimes = 0;
        PatternNodeKey nodeKey = new PatternNodeKey(levelKey.getProjectName(), levelKey.getLevel());
        PatternNode parentNode= new PatternNode(tokens);
        do {
            long timestamp = addNode(nodeKey, parentNode, getMaxUpdatedTime(levelKey));
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

    public Pair<PatternNodeKey, List<String>> mergePatternToNode(PatternNodeKey key,
                                                                 List<String> patternTokens,
                                                                 double maxDist) {
        PatternNode parentNode = patternNodes.get(key);
        try {
            List<String> mergedTokens = retrievePattern(parentNode.getPatternTokens(), patternTokens);
            if (!mergedTokens.equals(parentNode.getPatternTokens())) {
                parentNode.updatePatternTokens(mergedTokens);
                if (!parentNode.hasParent()) {
                    PatternNodeKey grandNodeKey = PatternLevelTree.getInstance()
                            .getParentNodeId(mergedTokens, key.getProjectName(), key.getLevel() + 1, maxDist);
                    parentNode.setParent(grandNodeKey);
                }
                //update the tree node(parent Id and pattern) by key
                if (parentNode.hasParent() && updateNode(key, parentNode) > 0) {
                    return Pair.of(parentNode.getParentId(), mergedTokens);
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return null;
    }

    public Set<String> getAllProjectsName() {
        Set<String> projectList = new HashSet<>();
        for (PatternNodeKey key : patternNodes.keySet()) {
            projectList.add(key.getProjectName());
        }
        return projectList;
    }

    public long addNode(PatternNodeKey nodeKey, PatternNode node,
                        long clientlastUpdatedTime) {
        //TODO: get maxUpdatedTime From DB
        long maxUpdateTime = getMaxUpdatedTime(nodeKey.getLevelKey());
        //client need to synchronize
        if (clientlastUpdatedTime < maxUpdateTime) {
            return 0;
        }

        node.setLastupdatedTime(System.currentTimeMillis());

        patternNodes.put(nodeKey, node);
        return node.getLastupdatedTime();
    }

    private long updateNode(PatternNodeKey nodeKey, PatternNode node) {
        if (!patternNodes.containsKey(nodeKey)) {
            return 0;
        }

        //TODO:udpate Node to DB,
        // if DB disconnected, then what?
        node.setLastupdatedTime(System.currentTimeMillis());
        patternNodes.put(nodeKey, node);
        return node.getLastupdatedTime();
    }

    private Map<PatternNodeKey, PatternNode> getNodes(PatternLevelKey levelKey) {
        //For java pass object by reference
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
            if (entry.getKey().getLevelKey().equals(levelKey)) {
                nodes.put(entry.getKey(), entry.getValue());
            }
        }
        return nodes;
    }


    private int getProjectMaxLevel(String name) {
        int maxLevel = -1;
        for (PatternNodeKey key : patternNodes.keySet()) {
            if (StringUtils.equals(key.getProjectName(), name)) {
                int level = key.getLevel();
                maxLevel = level > maxLevel ? level : maxLevel;
            }
        }
        return maxLevel;
    }

    private long getMaxUpdatedTime(PatternLevelKey levelKey) {
        long maxUpdateTime = 0;
        for (Map.Entry<PatternNodeKey, PatternNode> nodeEntry : patternNodes.entrySet()) {
            if (levelKey.equals(nodeEntry.getKey().getLevelKey())) {
                maxUpdateTime = maxUpdateTime < nodeEntry.getValue().getLastupdatedTime() ?
                        nodeEntry.getValue().getLastupdatedTime() : maxUpdateTime;
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
        for (Map.Entry<PatternNodeKey, PatternNode> entryNode : patternNodes.entrySet()) {
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
        return stringBuilder.toString();
    }
}
