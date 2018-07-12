package com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.common.preprocess.Tokenizer;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/7/8 0008.
 */
public class PatternNodeClientTest {
    private static PatternNodeClient client = new PatternNodeClient("localhost:7911",
            PatternNodeCenterType.HDFS);

    @Test
    public void startClientTest() {
        try {
            Map<PatternNodeKey, PatternNode> nodes = client.getNodes("syslog", 0);
            for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
                System.out.println(entry.getKey());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startClientTest1() {
        try {
            Map<PatternNodeKey, PatternNode> nodes = client.synchronizeNodes("syslog", 0, 0);
            for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
                System.out.println(entry.getKey());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startClientTest3() {
        try {
            String log = "this is a syslog log";
            PatternNodeKey nodeKey = new PatternNodeKey("syslog", 0);
            PatternNode node = new PatternNode(Tokenizer.simpleTokenize(log));
            long timestamp = client.addNode(nodeKey, node, 0);
            System.out.println(timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startClientTest4() {
        try {
            Map<PatternNodeKey, PatternNode> nodes = client.getNodes("syslog", 0);
            for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
                long timestamp = client.updateNode(entry.getKey(), entry.getValue());
                System.out.println(timestamp);
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void jsonparseTest() {
        String result = "{\"count\":1,\"sources\":{\"syslog#@#0#@#1fe5391fbb294eff885653980845b601\":{\"lastupdatedTime\":1531119075312,\"representTokens\":[\"this\",\" \",\"is\",\" \",\"a\",\" \",\"syslog\",\" \",\"log\"],\"patternTokens\":[\"this\",\" \",\"is\",\" \",\"a\",\" \",\"syslog\",\" \",\"log\"]}}}";
        Map<String, PatternNode> nodes = new HashMap<>();

        JsonParser jsonParser = new JsonParser();
        Gson gson  = new Gson();
        JsonObject jsonObject = jsonParser.parse(result).getAsJsonObject();
        long count = jsonObject.get("count").getAsLong();
        Type nodesType = new TypeToken<Map<String, PatternNode>>() {}.getType();
        System.out.println(jsonObject.get("sources").toString());
        try {
        nodes = gson.fromJson(
                jsonObject.get("sources").getAsJsonObject(), nodesType);
            PatternNodeKey nodeKey;
            for (Map.Entry<String, PatternNode> entry : nodes.entrySet()) {
                System.out.println("key class :" + entry.getKey().getClass() + "key : " + entry.getKey());
                System.out.println("value class : " + entry.getValue().getClass() + "value: " + entry.getValue());
                break;
            }
        } catch (Exception e) {
           e.printStackTrace();
        }
    }


}
