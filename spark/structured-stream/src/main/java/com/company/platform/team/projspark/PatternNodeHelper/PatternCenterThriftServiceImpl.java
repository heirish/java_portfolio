package com.company.platform.team.projspark.PatternNodeHelper;

import com.company.platform.team.projspark.common.data.PatternNode;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.thrift.TException;

import java.util.Map;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class PatternCenterThriftServiceImpl implements PatternCenterThriftService.Iface {
    private static final Gson gson = new Gson();
    private static final JsonParser jsonParser = new JsonParser();

    @Override
    public String getNodes(String projectName, int nodeLevel) throws TException {
        Map<PatternNodeKey, PatternNode> nodes = TestCenter.getInstance()
                .getNodes(projectName, nodeLevel);
        JsonObject object=new JsonObject();
        object.addProperty("count", nodes.size());
        object.add("sources", gson.toJsonTree(nodes));
        return object.toString();
    }

    @Override
    public String synchronizeNodes(String projectName,
                                   int nodeLevel,
                                   long latestUpdatedTime) throws TException {
        Map<PatternNodeKey, PatternNode> nodes = TestCenter.getInstance()
                .getNodesNewerThan(projectName, nodeLevel, latestUpdatedTime);
        JsonObject object=new JsonObject();
        object.addProperty("count", nodes.size());
        object.add("sources", gson.toJsonTree(nodes));
        return object.toString();
    }

    @Override
    public String addNode(String nodeInfo, long latestUpdatedTime) throws TException {
        JsonObject jsonObject = jsonParser.parse(nodeInfo).getAsJsonObject();
        PatternNodeKey nodeKey = gson.fromJson(jsonObject.get("nodeKey"), PatternNodeKey.class);
        PatternNode node = gson.fromJson(jsonObject.get("nodeValue"), PatternNode.class);
        return String.valueOf(TestCenter.getInstance()
                .addNode(nodeKey, node, latestUpdatedTime));
    }

    @Override
    public String updateNode(String nodeInfo) throws TException {
        JsonObject jsonObject = jsonParser.parse(nodeInfo).getAsJsonObject();
        PatternNodeKey nodeKey = gson.fromJson(jsonObject.get("nodeKey"), PatternNodeKey.class);
        PatternNode node = gson.fromJson(jsonObject.get("nodeValue"), PatternNode.class);
        return String.valueOf(TestCenter.getInstance()
                .updateNode(nodeKey, node));
    }
}
