package com.company.platform.team.projspark.utils;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by admin on 2018/6/22.
 */
public class ListUtilTest {
    private static final Logger logger = Logger.getLogger("");

    @Test
    public void removeExcessiveDuplicatesTest(){
        List<String> tokens = Arrays.asList(new String[] {" ", " ", " ", "abc"," ", " ", "esdf", "*", ",", "."});
        List<String> newTokens = ListUtil.removeExcessiveDuplicates(tokens, " ", 1);
        logger.info(newTokens);
        logger.info(String.join("", newTokens));
    }
}
