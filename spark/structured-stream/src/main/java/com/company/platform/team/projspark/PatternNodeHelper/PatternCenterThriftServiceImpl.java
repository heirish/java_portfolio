package com.company.platform.team.projspark.PatternNodeHelper;

import com.company.platform.team.projspark.PatternRefiner.PatternLevelTree;
import com.company.platform.team.projspark.common.data.PatternLevelKey;
import com.company.platform.team.projspark.common.data.PatternNode;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.thrift.TException;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class PatternCenterThriftServiceImpl implements PatternCenterThriftService.Iface {
    private static final Gson gson = new Gson();
    private static final JsonParser jsonParser = new JsonParser();

    @Override
    public String getNodes(String projectName, int nodeLevel) throws TException {
        try {
            Map<PatternNodeKey, PatternNode> nodes = PatternLevelTree.getInstance()
                    .getNodes(new PatternLevelKey(projectName, nodeLevel));
            JsonObject object = new JsonObject();
            Type nodesType = new TypeToken<Map<PatternNodeKey, PatternNode>>() {}.getType();
            object.addProperty("count", nodes.size());
            object.add("sources", gson.toJsonTree(nodes, nodesType).getAsJsonObject());
            return object.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    @Override
    public String synchronizeNodes(String projectName,
                                   int nodeLevel,
                                   long latestUpdatedTime) throws TException {
        try {
            Map<PatternNodeKey, PatternNode> nodes = PatternLevelTree.getInstance()
                    .getNodesNewerThan(projectName, nodeLevel, latestUpdatedTime);
            JsonObject object = new JsonObject();
            object.addProperty("count", nodes.size());
            object.add("sources", gson.toJsonTree(nodes));
            return object.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    @Override
    public String addNode(String nodeInfo, long latestUpdatedTime) throws TException {
        try {
            System.out.println("received nodeInfo: " + nodeInfo);
            JsonObject jsonObject = jsonParser.parse(nodeInfo).getAsJsonObject();
            PatternNodeKey nodeKey = PatternNodeKey.fromString(jsonObject.get("nodeKey").getAsString());
            PatternNode node = gson.fromJson(jsonObject.get("nodeValue"), PatternNode.class);
            long timestamp = PatternLevelTree.getInstance().addNode(nodeKey, node, latestUpdatedTime);
            System.out.println("add Node succeed? " + timestamp);
            return String.valueOf(timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    @Override
    public String updateNode(String nodeInfo) throws TException {
        try {
            System.out.println("updateNode, received nodeInfo: " + nodeInfo);
            JsonObject jsonObject = jsonParser.parse(nodeInfo).getAsJsonObject();
            PatternNodeKey nodeKey = PatternNodeKey.fromString(jsonObject.get("nodeKey").getAsString());
            PatternNode node = gson.fromJson(jsonObject.get("nodeValue"), PatternNode.class);
            long timestamp = PatternLevelTree.getInstance().updateNode(nodeKey, node);
            System.out.println("update Node succeed? " + timestamp);
            return String.valueOf(timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
