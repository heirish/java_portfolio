package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.stormtopology.data.PatternMetas;
import org.junit.Test;

/**
 * Created by admin on 2018/8/7.
 */
public class PatternMetasTest {
    private static PatternMetas patternMetas = PatternMetas.getInstance(null, null);

    @Test
    public void getSimilarityTest() {
        String projectName = "test";

        for (int i=0; i< 11; i++) {
            PatternLevelKey levelKey = new PatternLevelKey(projectName, i);
            System.out.println(i + ": " + patternMetas.getProjectSimilarity(levelKey));
        }
    }
}
