package com.company.platform.team.projpatternreco.stormtopology;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2018/7/25.
 */
public class UsageTest {

    @Test
    public void mapDifferenceTest() {
        Map<String, String> oldMap = null;
        Map<String, String> newMap= null;
        MapDifference<String, String> diff;
        //diff = Maps.difference(oldMap, newMap);
        System.out.println("test");

        oldMap = new HashMap<>();
        newMap = new HashMap<>();

        oldMap.put("aa", "123123");
        oldMap.put("bb", "111");
        oldMap.put("cc", "2222");

        newMap.put("bb", "333");
        newMap.put("cc", "2222");
        newMap.put("dd", "4444");

        diff = Maps.difference(oldMap, newMap);
        System.out.println(diff.entriesOnlyOnLeft().keySet().toString());
        System.out.println(diff.entriesOnlyOnRight().keySet().toString());
        System.out.println(diff.entriesDiffering().keySet().toString());
        System.out.println(diff.entriesInCommon().keySet().toString());
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

    @Test
    public void decimalPreciesTest() {
        double  a = 0.14159;
        BigDecimal bd = new BigDecimal(a).setScale(15, RoundingMode.HALF_EVEN);
        double value = bd.doubleValue();
        String strValue = String.valueOf(value);
        System.out.println(value);
        System.out.println(strValue);
        System.out.println(Double.parseDouble(strValue));


        double b= 0.1413333;
        BigDecimal  bb = new BigDecimal(b);
        if (StringUtils.equals(bd.setScale(2, RoundingMode.HALF_EVEN).toString(),
                bb.setScale(2, RoundingMode.HALF_EVEN).toEngineeringString())) {
            System.out.println("equal");
        }
    }
}
