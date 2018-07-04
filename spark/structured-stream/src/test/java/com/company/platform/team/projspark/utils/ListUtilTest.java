package com.company.platform.team.projspark.utils;

import com.company.platform.team.projspark.data.Constants;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static void printMyMap(Map<String, Map<String, String>> myMap) {
        for(Map.Entry<String, Map<String, String>> entry : myMap.entrySet()) {
            System.out.println("level: " + entry.getKey());
            for(Map.Entry<String, String> itemEntry : entry.getValue().entrySet()) {
                System.out.println("\t key:" + itemEntry.getKey() + ", value:" + itemEntry.getValue());
            }
        }
    }

    private static void modifyMap(Map<String, String> inMap) {
        inMap.put("mykey", "myvalue");
    }

    private static void modifyList(List< String> inList) {
       inList.add("awefas");
    }

    @Test
    public void nestedMapTest() {
        Map<String, Map<String, String>> myMap = new HashMap<>();
        for (int i=0; i< 10; i++) {
            Map<String, String> myMapItem = new HashMap<>();
            myMapItem.put("id0", "value");
            myMapItem.put("id1", "value");
            myMapItem.put("id2", "value");
            myMap.put(String.format("%s", i), myMapItem);
        }
        printMyMap(myMap);

        //Modify
        Map<String, String> localMap = myMap.get("9");
        myMap.get("9").put("id3", "value");
        //can add/update by get and put
        printMyMap(myMap);
        //modify by other function
        modifyMap(localMap);
        printMyMap(myMap);

        for(Map.Entry<String, String> entry:localMap.entrySet()) {
            System.out.println("local key:" + entry.getKey() + ", local value: " + entry.getValue());
        }
    }

    @Test
    public void regexTest()
    {
        Pattern pattern = Pattern.compile("(.*.json)|(.*/patternoutput-.*/part-.*)");
        //String path = "/home/heirish/Myproj/structured_stream/patternbase/part-00003-87342fec-4817-4e83-b492-35e13fd05196-c000.json";
        String path = "/home/heirish/Myproj/structured_stream/patternoutput-0/part-r-00000";
        Matcher m = pattern.matcher(path);
        System.out.println("Is path:" + path + " matching "
                + " ?, " + m.matches());
    }
}
