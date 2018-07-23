package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.stormtopology.leaffinder.PatternLeaves;
import com.company.platform.team.projpatternreco.stormtopology.refinder.PatternNodes;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
            PatternLeaves.getInstance().addNode(nodeKey,
                    new PatternNode(tokens));
            for (int i=0; i< 1; i++) {
                nodeKey = PatternNodeKey.fromString(nodeKeyString + i);
                PatternLeaves.getInstance().addNode(nodeKey,
                        new PatternNode(tokens));
            }
            nodeKey = PatternNodeKey.fromString("test#@#0#@#17fda459602e42b49c6061c90f7a0b090");
            PatternNode node = PatternNodes.getInstance().getNode(nodeKey);
            System.out.println(node.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void addNewPatternTest() {
        double leafSimilarity = 0.9;
        double decayRefactor = 0.9;
        PatternNodeKey parentNodeKey = new PatternNodeKey("test", 0);
        List<String> parentTokens = Arrays.asList("that is a test".split(" "));
        List<String> patternTokens = Arrays.asList("this is a test".split(" "));


        PatternLeaves.getInstance().addNode(parentNodeKey,
                new PatternNode(parentTokens));

        try {
            for (int i = 1; i < 11; i++) {
                double maxDist = 1 - leafSimilarity * Math.pow(decayRefactor, i);
                boolean isLastLevel = (i==10) ? true : false;
                Pair<PatternNodeKey, List<String>> nextLevelTuple = PatternNodes.getInstance()
                            .mergePatternToNode(parentNodeKey, patternTokens, maxDist, isLastLevel);
                if (nextLevelTuple == null) {
                    break;
                }
                parentNodeKey = nextLevelTuple.getLeft();
                patternTokens = nextLevelTuple.getRight();
                System.out.println(parentNodeKey.toString());
                System.out.println(Arrays.toString(patternTokens.toArray()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
