package com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper;

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
