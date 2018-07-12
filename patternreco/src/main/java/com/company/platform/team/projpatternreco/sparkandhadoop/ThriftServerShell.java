package com.company.platform.team.projpatternreco.sparkandhadoop;

import com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper.PatternNodeCenterType;
import com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper.PatternNodeServer;

/**
 * Created by admin on 2018/7/9.
 */
public class ThriftServerShell {
    public static void main(String[] args) {
        System.out.println("Main Method Entry");

        try {
            new PatternNodeServer("localhost:7911",
                    //new PatternNodeServer("10.34.130.93:7911",
                    PatternNodeCenterType.HDFS, 1).start();
            Thread.sleep(3000);
        } catch (InterruptedException x) {
        } catch (Exception e) {
        }

        System.out.println("Main Method Exit");
    }
}
