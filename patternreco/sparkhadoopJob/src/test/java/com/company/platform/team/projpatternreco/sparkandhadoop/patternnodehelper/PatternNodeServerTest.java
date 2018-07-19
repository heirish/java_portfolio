package com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper;

import org.junit.Test;

/**
 * Created by Administrator on 2018/7/8 0008.
 */
public class PatternNodeServerTest {

    @Test
    public void startServerTest() {
        try {
            new PatternNodeServer("localhost:7911",
                    PatternNodeCenterType.HDFS, 1).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Test
    //public void startServerTest2() {
    //    try {
    //        new PatternNodeThriftServer("192.168.152.158:7911", 2)
    //                .startThreadServer();
    //    } catch (Exception e) {
    //        e.printStackTrace();
    //    }
    //}
}
