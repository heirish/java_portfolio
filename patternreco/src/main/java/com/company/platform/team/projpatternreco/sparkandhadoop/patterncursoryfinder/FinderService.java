package com.company.platform.team.projpatternreco.sparkandhadoop.patterncursoryfinder;


/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class FinderService {
    private String serviceName;
    private ServiceType serviceType;
    private String confFileName;

    public FinderService(String name, ServiceType serviceType, String confFileName) {
       this.serviceName = name;
       this.serviceType = serviceType;
       this.confFileName = confFileName;
    }

    public void run() throws Exception{
        if (confFileName == null) {
            throw new Exception ("invalid configuration file");
        }

        if (serviceType == ServiceType.SPARK) {
            FinderServiceConfigure conf = FinderServiceConfigure.parseFromJson(confFileName);
            SparkFinder.startWork(conf, serviceName);
        }
    }
}
