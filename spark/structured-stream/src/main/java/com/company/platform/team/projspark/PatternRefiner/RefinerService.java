package com.company.platform.team.projspark.PatternRefiner;


import com.company.platform.team.projspark.PatternCursoryFinder.FinderServiceConfigure;
import com.company.platform.team.projspark.PatternCursoryFinder.ServiceType;
import com.company.platform.team.projspark.PatternCursoryFinder.SparkFinder;
import com.company.platform.team.projspark.common.utils.FluentScheduledExecutorService;

import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2018/7/7 0007.
 */
public class RefinerService {
    private RefinerServiceConfigure conf;
    private String serviceName;

    public RefinerService(RefinerServiceConfigure conf, String name) {
       this.conf = conf;
       this.serviceName = name;
    }

    public void run() throws Exception{
        if (conf == null) {
            throw new Exception ("invalid configuration");
        }
        new FluentScheduledExecutorService(1)
                .scheduleWithFixedDelay(new MapReduceRefiner(conf, "patternretrieve"),
                        conf.getInitialDelaySeconds(), conf.getPeriodSeconds(),
                        TimeUnit.SECONDS);
    }
}
