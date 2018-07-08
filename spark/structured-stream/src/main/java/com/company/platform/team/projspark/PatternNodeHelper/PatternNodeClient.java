package com.company.platform.team.projspark.PatternNodeHelper;

import com.company.platform.team.projspark.common.data.PatternNode;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class PatternNodeClient {
    private String serverAddress;
    private PatternNodeCenterType serverType;
    private static final Gson gson = new Gson();
    private static final JsonParser jsonParser = new JsonParser();

    public PatternNodeClient(String serverAddress, PatternNodeCenterType serverType) {
        this.serverAddress = serverAddress;
        this.serverType = serverType;
    }

    private PatternCenterThriftService.Client createThriftClient() throws Exception {
            String items[] = serverAddress.split(":");
            TTransport tTransport = new TSocket(items[0], Integer.parseInt(items[1]));
            //协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(tTransport);
            PatternCenterThriftService.Client client = new PatternCenterThriftService.Client(protocol);
            tTransport.open();
            return client;
    }

    public Map<PatternNodeKey, PatternNode> getNodes(String projectName, int nodeLevel) throws Exception{
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        String result = "";
        if (serverType == PatternNodeCenterType.HBASE ||
                serverType == PatternNodeCenterType.HDFS) {
           result = createThriftClient().getNodes(projectName, nodeLevel);
        }

        JsonObject jsonObject = jsonParser.parse(result).getAsJsonObject();
        long count = jsonObject.get("count").getAsLong();
        nodes = gson.fromJson(
                jsonObject.get("sources").toString(), nodes.getClass());
        if (count != nodes.size()) {
            throw new Exception("Currupted return");
        }

        return nodes;
    }

    public Map<PatternNodeKey, PatternNode> synchronizeNodes(String projectName,
                                                             int nodeLevel,
                                                             long latestUpdatedTime) throws Exception{
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        String result = "";
        if (serverType == PatternNodeCenterType.HBASE ||
                serverType == PatternNodeCenterType.HDFS) {
            result = createThriftClient()
                    .synchronizeNodes(projectName, nodeLevel, latestUpdatedTime);
        }

        JsonObject jsonObject = jsonParser.parse(result).getAsJsonObject();
        long count = jsonObject.get("count").getAsLong();
        nodes = gson.fromJson(
                jsonObject.get("sources").toString(), nodes.getClass());
        if (count != nodes.size()) {
            throw new Exception("Currupted return");
        }

        return nodes;
    }

    public long addNode(PatternNodeKey nodeKey, PatternNode node, long lastUpdatedTime) throws Exception{
        JsonObject object=new JsonObject();
        object.addProperty("nodeKey", nodeKey.toString());
        object.add("nodeValue", gson.toJsonTree(node));
        String nodeInfo = object.toString();

        if (serverType == PatternNodeCenterType.HBASE ||
                serverType == PatternNodeCenterType.HDFS) {
            return Long.parseLong(createThriftClient()
                    .addNode(nodeInfo, lastUpdatedTime));
        }
        return 0;
    }

    public long updateNode(PatternNodeKey nodeKey, PatternNode node) throws Exception{
        JsonObject object=new JsonObject();
        object.addProperty("nodeKey", nodeKey.toString());
        object.add("nodeValue", gson.toJsonTree(node));
        String nodeInfo = object.toString();
        if (serverType == PatternNodeCenterType.HBASE ||
                serverType == PatternNodeCenterType.HDFS) {
            return Long.parseLong(createThriftClient().updateNode(nodeInfo));
        }
        return 0;
    }
}
