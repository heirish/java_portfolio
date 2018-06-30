package com.company.platform.team.projspark.utils;

import com.company.platform.team.projspark.data.Constants;
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

    @Test
    public void splitWithStringTest()
    {
        String key = String.format("%s%s%s%s%s",
                "test", Constants.PATTERN_NODE_KEY_DELIMITER,
                "0", Constants.PATTERN_NODE_KEY_DELIMITER,
                "waefsdf");
        String[] fields = key.split(Constants.PATTERN_NODE_KEY_DELIMITER);
        System.out.println(key.split(Constants.PATTERN_NODE_KEY_DELIMITER)[2]);
        System.out.println(Arrays.toString(fields));
    }
}
