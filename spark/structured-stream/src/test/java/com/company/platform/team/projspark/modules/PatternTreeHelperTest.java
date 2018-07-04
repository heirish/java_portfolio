package com.company.platform.team.projspark.modules;

import com.company.platform.team.projspark.data.PatternNode;
import org.junit.Test;

import java.util.Map;

/**
 * Created by admin on 2018/7/4.
 */
public class PatternTreeHelperTest {
    @Test
    public void getAllNodesTest() {
        PatternTreeHelper helper = new PatternTreeHelper();
        Map<String, PatternNode> nodes = helper.getAllNodes();
        for (Map.Entry<String, PatternNode> entry : nodes.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue().getRepresentTokens());
            System.out.println(entry.getValue().getPatternTokens());
        }
    }
}
