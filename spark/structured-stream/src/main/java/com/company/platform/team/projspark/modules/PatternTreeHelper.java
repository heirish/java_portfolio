package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.data.PatternNode;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/29.
 */
public class PatternTreeHelper {
    //SAVE to hdfs, because the tree might grow very fast, concern about the data size
    private String lastUpdatedTime = "";
    private static final Gson gson = new Gson();

    public Map<String, PatternNode> getAllNodes() {
        Map<String, PatternNode> nodes = new HashMap<>();
        //TODO:read from all level directory
        String file = "./patternLeaves";
        try (BufferedReader br = new BufferedReader(new FileReader(file))){
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                Map<String, String> fields = gson.fromJson(line, Map.class);
                String key = fields.get(Constants.FIELD_PATTERNID);
                List<String> patternTokens= Arrays.asList(fields.get(Constants.FIELD_PATTERNTOKENS)
                        .split(Constants.PATTERN_NODE_KEY_DELIMITER));
                List<String> representTokens = Arrays.asList(fields.get(Constants.FIELD_REPRESENTTOKENS)
                        .split(Constants.PATTERN_NODE_KEY_DELIMITER));
                PatternNode node = new PatternNode(representTokens);
                node.updatePatternTokens(patternTokens);
                nodes.put(key, node);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        lastUpdatedTime = String.valueOf(System.currentTimeMillis());
        return nodes;
    }

    public Map<String, PatternNode> getAllLeaves() {
        Map<String, PatternNode> nodes = new HashMap<>();
        lastUpdatedTime = String.valueOf(System.currentTimeMillis());
        return nodes;
    }

    //get All modified nodes since lastupdatedTime
    public Map<String, PatternNode> getNewNodes() {
        Map<String, PatternNode> nodes = new HashMap<>();
        return nodes;
    }

    //get All modified nodes since lastupdatedTime
    public Map<String, PatternNode> getNewLeaves() {
        Map<String, PatternNode> nodes = new HashMap<>();
        return nodes;
    }

    public boolean addNodesToCenter(String projectName, int nodeLevel, String nodeId, PatternNode node) {
        // TODO:
        // 1. send lastUpdatedTime and nodeinfo to Center
        // 2. if center return tells that need to synchronize
        //        synchronize and return false
        // 3. else return what center returned, true - center added succeed, false - center added failure
        String key = String.format("%s%s%s%s%s",
                projectName, Constants.PATTERN_NODE_KEY_DELIMITER,
                nodeLevel, Constants.PATTERN_NODE_KEY_DELIMITER,
                nodeId);
        return true;
    }

    public boolean updateNodesToCenter(String projectName, int nodeLevel, String nodeId, PatternNode node) {
        // TODO:
        return true;
    }
}
