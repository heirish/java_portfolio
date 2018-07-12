package com.company.platform.team.projpatternreco.sparkandhadoop.patternrefiner;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.sparkandhadoop.patternerfiner.PatternLevelTree;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/7/6 0006.
 */
public class PatternLevelTreeTest {
    private static final Gson gson = new Gson();

    @Test
    public void saveTreeToFileTest(){
        PatternLevelTree.getInstance().backupTree("test/LevelTreeSaveTest");
    }

    @Test
    public void backupTreeTest(){
        PatternLevelTree.getInstance().backupTree("test/LevelTreebakTest");
    }

    @Test
    public void jsonSerializeTest() {
        Map<PatternNodeKey, PatternNode> nodes = PatternLevelTree
                .getInstance().getNodes(new PatternLevelKey("test", 0));
        try {
            FileWriter fw = new FileWriter("test/leveltreeserializeTest");
            String treeString = gson.toJson(nodes);
            JsonObject object=new JsonObject();
            object.addProperty("count", nodes.size());
            object.add("sources", gson.toJsonTree(nodes));
            fw.write(object.toString());
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void jsonSerializeTest2() {
        Map<PatternNodeKey, PatternNode> nodes = PatternLevelTree
                .getInstance().getNodes(new PatternLevelKey("test", 0));
        try {
            PatternNodeKey nodeKey;
            PatternNode node;
            FileWriter fw = new FileWriter("test/leveltreeserializeTest2");
            for(Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
                nodeKey = entry.getKey();
                node = entry.getValue();
                JsonObject object=new JsonObject();
                object.addProperty("key", nodeKey.toString());
                object.add("value", gson.toJsonTree(node));
                fw.write(object.toString());
                break;
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void jsonDeserializeTest() {
        Map<PatternNodeKey, PatternNode> nodes = PatternLevelTree
                .getInstance().getNodes(new PatternLevelKey("test", 0));
        try {
            FileWriter fw = new FileWriter("test/leveltreeDeserializeTest");
            JsonObject object=new JsonObject();
            object.addProperty("count", nodes.size());
            object.add("sources", gson.toJsonTree(nodes));
            String treeString = object.toString();
            JsonParser parser = new JsonParser();
            JsonObject jsonObject = parser.parse(treeString).getAsJsonObject();
            System.out.println(jsonObject.get("count"));
            Map<PatternNodeKey, PatternNode> nodeItems = new HashMap<>();
            nodeItems = gson.fromJson(
                    jsonObject.get("sources").toString(), nodeItems.getClass());

            //Set<String> keys = jsonObject.keySet();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }}
