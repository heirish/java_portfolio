package com.company.platform.team.projspark.PatternCursoryFinder;


/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class FinderService {
    private FinderServiceConfigure conf;
    private String serviceName;

    public FinderService(FinderServiceConfigure conf, String name) {
       this.conf = conf;
       this.serviceName = name;
    }

    public void run() throws Exception{
        if (conf == null) {
            throw new Exception ("invalid configuration");
        }

        if (ServiceType.fromString(conf.getServiceType()) == ServiceType.SPARK) {
            SparkFinder.startWork(conf, serviceName);
        }
    }
}
