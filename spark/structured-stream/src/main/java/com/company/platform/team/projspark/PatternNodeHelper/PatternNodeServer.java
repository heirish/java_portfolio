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
    private int nThread;

    public PatternNodeServer(String serverAddress, PatternNodeCenterType centerType, int nThread) {
        this.serverAddress = serverAddress;
        this.centerType = centerType;
        this.nThread = nThread;
    }

    public void start() throws Exception {
        if (centerType == PatternNodeCenterType.HBASE
                || centerType == PatternNodeCenterType.HDFS) {
            PatternNodeThriftServer server = new PatternNodeThriftServer(serverAddress);
            server.setDaemon(false);
            server.start();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException x) {
            }
        }
    }
}
