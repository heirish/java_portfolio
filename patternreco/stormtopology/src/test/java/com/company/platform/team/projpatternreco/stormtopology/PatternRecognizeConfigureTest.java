package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.stormtopology.data.RunningType;
import org.junit.Test;

import java.util.Map;

/**
 * Created by admin on 2018/7/24.
 */
public class PatternRecognizeConfigureTest {

    @Test
    public void parseIntTest() {
        PatternRecognizeConfigure conf = PatternRecognizeConfigure.getInstance("PatternRecognize-com.json", RunningType.LOCAL);
        Map redisMap = conf.getConfigMap("redis");
        System.out.println(redisMap.get("port"));
        try {
            System.out.println((int) Double.parseDouble(redisMap.get("port").toString()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
