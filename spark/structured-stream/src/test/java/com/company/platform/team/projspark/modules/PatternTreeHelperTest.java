package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.common.data.PatternNode;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import org.junit.Test;

import java.util.Map;

/**
 * Created by admin on 2018/7/4.
 */
public class PatternTreeHelperTest {
    @Test
    public void getAllNodesTest() {
        PatternTreeHelper helper = new PatternTreeHelper();
        Map<PatternNodeKey, PatternNode> nodes = helper.getAllNodes();
        for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
            System.out.println(entry.getKey().toString());
            System.out.println(entry.getValue().getRepresentTokens());
            System.out.println(entry.getValue().getPatternTokens());
        }
    }
}
