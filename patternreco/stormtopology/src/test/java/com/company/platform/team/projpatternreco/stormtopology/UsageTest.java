package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

/**
 * Created by admin on 2018/7/25.
 */
public class UsageTest {

    @Test
    public void mapDifferenceTest() {
        Map<PatternNodeKey, PatternNode> newLevelNodes = null;
        Map<PatternNodeKey, PatternNode> levelNodes = null;
        MapDifference<PatternNodeKey, PatternNode> diff = Maps.difference(levelNodes, newLevelNodes);
        System.out.println("test");
    }

    @Test
    public void exponentialDecayTest() {
        double initValue = 0.7;
        double decayFactor = 0.2;
        int levelMax = 10;
        for (int i=1; i<levelMax + 1; i++) {
            double value = initValue * Math.pow(1-decayFactor, i);
            System.out.println(i + "th value: " + value);
        }
    }
}
