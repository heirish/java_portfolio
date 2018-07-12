package com.company.platform.team.projpatternreco.modules;

import com.company.platform.team.projpatternreco.common.data.Constants;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;

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

    public Map<PatternNodeKey, PatternNode> getAllNodes() {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        //TODO:read from all level directory
        String file = "tree/patternLeaves";
        //String file = "./patterntree";
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
        lastUpdatedTime = String.valueOf(System.currentTimeMillis());
        return nodes;
    }

    public Map<PatternNodeKey, PatternNode> getAllLeaves() {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
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

    public boolean addNodesToCenter(PatternNodeKey nodeKey, PatternNode node) {
        // TODO:
        // 1. send lastUpdatedTime and nodeinfo to Center
        // 2. if center return tells that need to synchronize
        //        synchronize and return false
        // 3. else return what center returned, true - center added succeed, false - center added failure
        return true;
    }

    public boolean updateNodesToCenter(PatternNodeKey nodeKey, PatternNode node) {
        // TODO:
        return true;
    }
}
