package com.company.platform.team.projspark.PatternNodeHelper;

import com.company.platform.team.projspark.common.data.Constants;
import com.company.platform.team.projspark.common.data.PatternLevelKey;
import com.company.platform.team.projspark.common.data.PatternNode;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
//SAVE to hdfs in production,
//because the tree might grow very fast, concern about the data size
public class TestCenter {
    private static TestCenter testCenter = new TestCenter();
    private static Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();

    private static final Gson gson = new Gson();

    public static TestCenter getInstance() {
        return testCenter;
    }

    private TestCenter() {
        String file = "./patternLeaves";
        //String file = "tree/patterntree";
        try (BufferedReader br = new BufferedReader(new FileReader(file))){
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    Map<String, String> fields = gson.fromJson(line, Map.class);
                    PatternNodeKey key = PatternNodeKey.fromString(fields.get(Constants.FIELD_PATTERNID));
                    //TODO:parentId is null
                    List<String> patternTokens = Arrays.asList(fields.get(Constants.FIELD_PATTERNTOKENS)
                            .split(Constants.PATTERN_NODE_KEY_DELIMITER));
                    List<String> representTokens = Arrays.asList(fields.get(Constants.FIELD_REPRESENTTOKENS)
                            .split(Constants.PATTERN_NODE_KEY_DELIMITER));
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
            e.printStackTrace();
        }
    }


    public Map<PatternNodeKey, PatternNode> getNodes(PatternLevelKey levelKey) {
        return getNodes(levelKey.getProjectName(), levelKey.getLevel());
    }

    public Map<PatternNodeKey, PatternNode> getNodes(String projectName, int nodeLevel) {
        Map<PatternNodeKey, PatternNode> returnNodes = new HashMap<>();
        for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
            if (StringUtils.equals(entry.getKey().getProjectName(), projectName)
                    && entry.getKey().getLevel() == nodeLevel) {
                returnNodes.put(entry.getKey(), entry.getValue());
            }
        }
        return returnNodes;
    }

    public Map<PatternNodeKey, PatternNode> getNodesNewerThan(String projectName,
                                                              int nodeLevel,
                                                              long lastUpdatedTime) {
        Map<PatternNodeKey, PatternNode> returnNodes = new HashMap<>();
        PatternLevelKey levelKey = new PatternLevelKey(projectName, nodeLevel);
        for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
            if (StringUtils.equals(entry.getKey().getProjectName(), projectName)
                    && entry.getKey().getLevel() == nodeLevel
                    && entry.getValue().getLastupdatedTime() >= lastUpdatedTime) {
                returnNodes.put(entry.getKey(), entry.getValue());
            }
        }
        return returnNodes;
    }

    public PatternNode getNode(PatternNodeKey nodeKey) {
        if (nodes.containsKey(nodeKey)) {
            return new PatternNode(nodes.get(nodeKey));
        } else {
            return null;
        }
    }

    private long getMaxUpdatedTime(String projectName, int nodeLevel) {
        long maxUpdateTime = 0;
        PatternLevelKey levelKey = new PatternLevelKey(projectName, nodeLevel);
        for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
            if (levelKey.equals(entry.getKey().getLevelKey())) {
                maxUpdateTime = maxUpdateTime < entry.getValue().getLastupdatedTime() ?
                        entry.getValue().getLastupdatedTime() : maxUpdateTime;
            }
        }
        return maxUpdateTime;
    }

    public long addNode(PatternNodeKey nodeKey, PatternNode node, long clientlastUpdatedTime) {
        if (nodes.containsKey(nodeKey)) {
            return 0;
        }

        long maxUpdateTime = getMaxUpdatedTime(nodeKey.getProjectName(),
                nodeKey.getLevel());
        //client need to synchronize
        if (clientlastUpdatedTime < maxUpdateTime) {
            return 0;
        }

        node.setLastupdatedTime(System.currentTimeMillis());
        nodes.put(nodeKey, node);
        return node.getLastupdatedTime();
    }

    public long updateNode(PatternNodeKey nodeKey, PatternNode node) {
        if (!nodes.containsKey(nodeKey)) {
            return 0;
        }
        node.setLastupdatedTime(System.currentTimeMillis());
        nodes.put(nodeKey, node);
        return node.getLastupdatedTime();
    }
}
