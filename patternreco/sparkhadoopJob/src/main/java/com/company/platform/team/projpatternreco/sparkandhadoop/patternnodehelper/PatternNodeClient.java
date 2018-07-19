package com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.Type;
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
    static class ThriftConnection {
        public TTransport tTransport;
        public PatternCenterThriftService.Client client;
    }

    public PatternNodeClient(String serverAddress, PatternNodeCenterType serverType) {
        this.serverAddress = serverAddress;
        this.serverType = serverType;
    }

    private ThriftConnection createThriftClient(){
        try {
            String items[] = serverAddress.split(":");
            TTransport tTransport = new TSocket(items[0], Integer.parseInt(items[1]));
            //协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(tTransport);
            PatternCenterThriftService.Client client = new PatternCenterThriftService.Client(protocol);
            tTransport.open();

            ThriftConnection thriftConnection = new ThriftConnection();
            thriftConnection.client = client;
            thriftConnection.tTransport = tTransport;

            return thriftConnection;
        } catch (Exception e) {
           e.printStackTrace();
        }
        return null;
    }

    private void destroyThriftClient(ThriftConnection thriftConnection) {
        thriftConnection.tTransport.close();
    }

    public Map<PatternNodeKey, PatternNode> getNodes(String projectName, int nodeLevel) throws Exception{
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        String result = "";
        if (serverType == PatternNodeCenterType.HBASE ||
                serverType == PatternNodeCenterType.HDFS) {
           ThriftConnection thriftConnection = createThriftClient();
           if (thriftConnection != null) {
               result = thriftConnection.client.getNodes(projectName, nodeLevel);
           } else {
               throw new Exception("can't connect to thrift server");
           }
           destroyThriftClient(thriftConnection);
        }

        JsonObject jsonObject = jsonParser.parse(result).getAsJsonObject();
        long count = jsonObject.get("count").getAsLong();
        Type nodesType = new TypeToken<Map<String, PatternNode>>() {}.getType();
        Map<String, PatternNode> nodesTmp = gson.fromJson(
                jsonObject.get("sources").getAsJsonObject(), nodesType);
        if (count != nodesTmp.size()) {
            System.out.println("count: " + count + ", mapsize: " +nodesTmp.size());
            throw new Exception("Currupted return");
        }

        for(Map.Entry<String, PatternNode> entry : nodesTmp.entrySet() ) {
            try {
                nodes.put(PatternNodeKey.fromString(entry.getKey()), entry.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
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
            ThriftConnection thriftConnection = createThriftClient();
            if (thriftConnection != null) {
                result = thriftConnection.client.synchronizeNodes(projectName, nodeLevel, latestUpdatedTime);
            } else {
                throw new Exception("can't connect to thrift server");
            }
            destroyThriftClient(thriftConnection);
        }

        JsonObject jsonObject = jsonParser.parse(result).getAsJsonObject();
        long count = jsonObject.get("count").getAsLong();

        Type nodesType = new TypeToken<Map<String, PatternNode>>() {}.getType();
        Map<String, PatternNode> nodesTmp = gson.fromJson(
                jsonObject.get("sources").getAsJsonObject(), nodesType);
        if (count != nodesTmp.size()) {
            System.out.println("count: " + count + ", mapsize: " +nodesTmp.size());
            throw new Exception("Currupted return");
        }

        for(Map.Entry<String, PatternNode> entry : nodesTmp.entrySet() ) {
            try {
                nodes.put(PatternNodeKey.fromString(entry.getKey()), entry.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return nodes;
    }

    public long addNode(PatternNodeKey nodeKey, PatternNode node, long lastUpdatedTime) throws Exception{
        JsonObject object=new JsonObject();
        object.addProperty("nodeKey", nodeKey.toString());
        object.add("nodeValue", gson.toJsonTree(node).getAsJsonObject());
        String nodeInfo = object.toString();
        System.out.println("nodeInfo: "  + nodeInfo);
        String result = "";
        if (serverType == PatternNodeCenterType.HBASE ||
                serverType == PatternNodeCenterType.HDFS) {
            ThriftConnection thriftConnection = createThriftClient();
            if (thriftConnection != null) {
                result = thriftConnection.client.addNode(nodeInfo, lastUpdatedTime);
            } else {
                throw new Exception("can't connect to thrift server");
            }
            destroyThriftClient(thriftConnection);
        }

        if (StringUtils.isNumeric(result)) {
            return Long.parseLong(result);
        }
        return 0;
    }

    public long updateNode(PatternNodeKey nodeKey, PatternNode node) throws Exception{
        JsonObject object=new JsonObject();
        object.addProperty("nodeKey", nodeKey.toString());
        object.add("nodeValue", gson.toJsonTree(node).getAsJsonObject());
        String nodeInfo = object.toString();
        System.out.println("Client, send nodeInfo: " + nodeInfo);
        String result = "";
        if (serverType == PatternNodeCenterType.HBASE ||
                serverType == PatternNodeCenterType.HDFS) {
            ThriftConnection thriftConnection = createThriftClient();
            if (thriftConnection != null) {
                result = thriftConnection.client.updateNode(nodeInfo);
            } else {
                throw new Exception("can't connect to thrift server");
            }
            destroyThriftClient(thriftConnection);
        }
        if (StringUtils.isNumeric(result)) {
            return Long.parseLong(result);
        }
        return 0;
    }
}
