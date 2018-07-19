package com.company.platform.team.projpatternreco.sparkandhadoop;

import com.company.platform.team.projpatternreco.common.utils.HdfsUtil;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;


/**
 * Created by Administrator on 2018/7/8 0008.
 */
public class HdfsUtilTest {
    @Test
    public void lastNstModifiedTest() {
        try {
            String filePath = "hdfs://localhost:9000/dataout/logpatternout";
            String filterRegex = ".*.json";
            String resourceFile = "/home/heirish/apps/hadoop/etc/hadoop/core-site.xml";
            FileStatus file = HdfsUtil.lastNstModified(filePath, 1,
                    filterRegex, resourceFile);
            System.out.println(file.getPath().toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void moveFilesAlreadyCompletedTest() {
        String filePath = "hdfs://localhost:9000/dataout/logpatternout";
        String filterRegex = ".*.json";
        String resourceFile = "/home/heirish/apps/hadoop/etc/hadoop/core-site.xml";
        String destPath = "hdfs://localhost:9000/dataout/patternrefined-0";
        int succeed = HdfsUtil.moveFilesAlreadyCompleted(filePath, destPath,
                filterRegex, resourceFile);
        System.out.println("moved scceed file num: " + succeed);
    }

    @Test
    public void deleteDirTest(){
        String path = "hdfs://localhost:9000/dataout/patternrefined-0";
        try {
            boolean ret = HdfsUtil.deleteDir(path);
            System.out.println(ret);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
