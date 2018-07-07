package com.company.platform.team.projspark.PatternNodeHelper;

import com.company.platform.team.projspark.common.data.Constants;
import com.company.platform.team.projspark.common.data.PatternNode;
import com.company.platform.team.projspark.common.data.PatternNodeKey;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class PatternNodeServer {
    private String serverAddress;
    private PatternNodeCenterType centerType;

    public PatternNodeServer(String serverAddress, PatternNodeCenterType centerType) {
        this.serverAddress = serverAddress;
        this.centerType = centerType;
    }

    public void start() throws Exception {
        if (centerType == PatternNodeCenterType.HBASE
                || centerType == PatternNodeCenterType.HDFS) {
            new PatternNodeThriftServer(serverAddress).start();
        }
    }
}
