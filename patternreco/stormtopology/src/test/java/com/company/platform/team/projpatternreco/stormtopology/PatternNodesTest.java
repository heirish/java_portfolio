package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.stormtopology.leaffinder.PatternLeaves;
import com.company.platform.team.projpatternreco.stormtopology.refinder.PatternNodes;
import org.junit.Test;

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
            //System.out.println(PatternLeaves.getInstance().toString());
            PatternNode node = PatternNodes.getInstance().getNode(nodeKey);
            //System.out.println(PatternNodes.getInstance().toString());
            System.out.println(node.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
