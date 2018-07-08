package com.company.platform.team.projspark.PatternNodeHelper;

import com.company.platform.team.projspark.common.data.PatternNode;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import org.junit.Test;

import java.util.Map;

/**
 * Created by Administrator on 2018/7/8 0008.
 */
public class PatternNodeClientTest {

    @Test
    public void startClientTest() {
        try {
            Map<PatternNodeKey, PatternNode> nodes = new PatternNodeClient("192.168.152.158:7911",
                    PatternNodeCenterType.HDFS).getNodes("test", 0);
            for (Map.Entry<PatternNodeKey, PatternNode> entry : nodes.entrySet()) {
                System.out.println(entry.getKey());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
