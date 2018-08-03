package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.stormtopology.utils.PatternNodes;
import com.company.platform.team.projpatternreco.stormtopology.utils.Recognizer;
import com.company.platform.team.projpatternreco.stormtopology.utils.RedisNodeCenter;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/7/15 0015.
 */
public class PatternNodesTest {
    @Test
    public void getNodesTest() {
        try {
            String nodeKeyString = "test#@#0#@#17fda459602e42b49c6061c90f7a0b09";
            PatternNodeKey nodeKey = PatternNodeKey.fromString(nodeKeyString);
            List<String> tokens = Arrays.asList("this is a test".split(" "));
            RedisNodeCenter.getInstance(config).updateNode(nodeKey,
                    new PatternNode(tokens));
            for (int i=0; i< 1; i++) {
                nodeKey = PatternNodeKey.fromString(nodeKeyString + i);
                RedisNodeCenter.getInstance(config).updateNode(nodeKey,
                        new PatternNode(tokens));
            }
            nodeKey = PatternNodeKey.fromString("test#@#0#@#17fda459602e42b49c6061c90f7a0b090");
            //PatternNode node = Recognizer.getInstance(config).getNode(nodeKey);
            //System.out.println(node.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void addNewPatternTest() {
        double leafSimilarity = 0.9;
        double decayRefactor = 0.9;
        PatternLevelKey levelKey= new PatternLevelKey("test", 0);
        List<String> parentTokens = Arrays.asList("that is a test".split(" "));
        List<String> patternTokens = Arrays.asList("this is a test".split(" "));


        PatternNodeKey parentNodeKey = Recognizer.getInstance(config).addNode(levelKey,
                new PatternNode(parentTokens));

        try {
            Recognizer.getInstance(config).mergeTokenToNode(parentNodeKey, patternTokens);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void addRedisNodeTest(){
        PatternLevelKey levelKey = new PatternLevelKey("test", 0);
        List<String> patternTokens = Arrays.asList("this is a test".split(" "));
        PatternNodeKey nodeKey =  RedisNodeCenter.getInstance(config).addNode(levelKey, new PatternNode(patternTokens));
        System.out.println(nodeKey.toString());
    }

    @Test
    public void autoNewFileTest() {
        try {
            String fileName = "test/test.txt";
            File file = new File(fileName);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            FileWriter fw = new FileWriter(fileName);
            try {
                fw.write("success");
            } catch (IOException e) {
                e.printStackTrace();
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
