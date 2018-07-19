package com.company.platform.team.projpatternreco.sparkandhadoop.patternnodehelper;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.*;

import java.net.InetSocketAddress;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class PatternNodeThriftServer extends Thread {
    private String host;
    private int port;

    public PatternNodeThriftServer(String ipPortAddress) throws Exception {
        String[] items = ipPortAddress.split(":");
        try {
            this.host = items[0];
            this.port = Integer.parseInt(items[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid ipPortAdress for PatternNodeThriftServer");
        }
    }

    public void run() {
        try {
            InetSocketAddress bindAddr = new InetSocketAddress(host, port);
            TServerSocket serverTransport = new TServerSocket(bindAddr);
            TServer.Args args = new TServer.Args(serverTransport);
            TProcessor process = new PatternCenterThriftService.Processor(
                    new PatternCenterThriftServiceImpl());
            TBinaryProtocol.Factory portFactory = new TBinaryProtocol.Factory(true, true);
            args.processor(process);
            args.protocolFactory(portFactory);

            TServer server = new TSimpleServer(args);
            System.out.println("start TServer");
            server.serve();
            System.out.println("TServer started....., on port :" + port);
        } catch (Exception e) {
            System.out.println(e);
            throw new RuntimeException(e);
        }
    }

    public void startThreadServer() throws Exception {
        InetSocketAddress bindAddr = new InetSocketAddress(host, port);
        TNonblockingServerTransport  serverTransport = new TNonblockingServerSocket(bindAddr);
        TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(serverTransport);
        TProcessor process = new PatternCenterThriftService.Processor(
                new PatternCenterThriftServiceImpl());
        //异步IO，需要使用TFramedTransport，它将分块缓存读取。
        TTransportFactory transportFactory = new TFramedTransport.Factory();
        //使用高密度二进制协议
        TProtocolFactory proFactory = new TCompactProtocol.Factory();
        args.processor(process);
        args.protocolFactory(proFactory);
        args.transportFactory(transportFactory);

        TServer server = new TThreadedSelectorServer(args);
        server.serve();
    }
}
