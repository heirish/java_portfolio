package com.company.platform.team.projspark.PatternNodeHelper;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

import java.net.InetSocketAddress;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class PatternNodeThriftServer {
    private String host;
    private int port;

    public PatternNodeThriftServer(String ipPortAddress) throws Exception{
        String[] items = ipPortAddress.split(":");
        try {
            host = items[0];
            port = Integer.parseInt(items[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid ipPortAdress for PatternNodeThriftServer");
        }
    }

    public void start() throws Exception{
        InetSocketAddress bindAddr = new InetSocketAddress(host, port);
        TServerSocket  serverTransport = new TServerSocket(bindAddr);
        TServer.Args args = new TServer.Args(serverTransport);
        TProcessor process = new PatternCenterThriftService.Processor(
                new PatternCenterThriftServiceImpl());
        TBinaryProtocol.Factory portFactory = new TBinaryProtocol.Factory(true, true);
        args.processor(process);
        args.protocolFactory(portFactory);

        TServer server = new TSimpleServer(args);
        server.serve();
    }
}
