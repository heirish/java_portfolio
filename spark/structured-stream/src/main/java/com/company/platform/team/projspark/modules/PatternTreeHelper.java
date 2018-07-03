package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.data.Constants;
import com.company.platform.team.projspark.data.PatternNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2018/6/29.
 */
public class PatternTreeHelper {
    private String lastUpdatedTime = "";

    public Map<String, PatternNode> getAllNodes() {
        Map<String, PatternNode> nodes = new HashMap<>();
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
        return true;
    }

    public boolean updateNodesToCenter(String projectName, int nodeLevel, String nodeId, PatternNode node) {
        // TODO:
        return true;
    }
}
