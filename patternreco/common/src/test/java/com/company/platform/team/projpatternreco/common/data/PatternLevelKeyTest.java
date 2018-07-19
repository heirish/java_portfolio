package com.company.platform.team.projpatternreco.common.data;

import org.junit.Test;

/**
 * Created by Administrator on 2018/7/6 0006.
 */
public class PatternLevelKeyTest {

    @Test
    public void toStringTest(){
        PatternLevelKey levelKey = new PatternLevelKey("syslog", 0);
        System.out.println(levelKey.toString());
    }

    @Test
    public void fromEmptyStringTest() {
        try {
            PatternLevelKey levelKey = PatternLevelKey.fromString("");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
