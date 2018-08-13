package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.stormtopology.PatternRecognizeConfigure;
import com.company.platform.team.projpatternreco.stormtopology.data.RunningType;
import org.junit.Test;

/**
 * Created by admin on 2018/8/13.
 */
public class RecognizerTest {
    @Test
    public void getInstanceTest(){
        String confFile = "PatternRecognize.json";
        RunningType runningType = RunningType.LOCAL;
        PatternRecognizeConfigure config = PatternRecognizeConfigure.getInstance(confFile, runningType);
        long startTime = System.currentTimeMillis();
        Recognizer recognizer = Recognizer.getInstance(config.getStormConfig());
        long endTime = System.currentTimeMillis();
        System.out.println("get instance takes " + (endTime - startTime) + " ms.");
    }
}
