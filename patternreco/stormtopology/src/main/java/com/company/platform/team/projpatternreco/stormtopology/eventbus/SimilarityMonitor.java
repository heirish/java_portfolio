package com.company.platform.team.projpatternreco.stormtopology.eventbus;

import com.company.platform.team.projpatternreco.stormtopology.data.RedisNodeCenter;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin on 2018/8/3.
 */
public final class SimilarityMonitor extends Thread{
    private static Logger logger = LoggerFactory.getLogger(SimilarityMonitor.class);
    private static final EventBus eventBusInstance = EventBus.getInstance();
    private static long INTERVAL_MILISECONDS_DEFAULT = 10;

    private RedisNodeCenter nodeCenter;
    private long interval;

    public SimilarityMonitor(Map conf) {
        try {
            interval = Long.parseLong(conf.get("similarityMonitorInterval").toString());
        } catch (Exception e) {
            interval = INTERVAL_MILISECONDS_DEFAULT;
            logger.warn("get similarityMonitorInterval from config failed, use default value:" + interval);
        }
        nodeCenter = RedisNodeCenter.getInstance(conf);
    }

    public void start() {
        Map<String, String> lastSimilarities = null;
        while (true) {
            try {
                Set<String> changedProjects = new HashSet<>();
                Map<String, String> currentSimilarities = null;
                //Map<String, String> currentSimilarities = nodeCenter.getSimilarityFingerPrints();
                if (currentSimilarities != null && lastSimilarities != null) {
                    MapDifference<String, String> difference = Maps.difference(lastSimilarities, currentSimilarities);
                    changedProjects.addAll(difference.entriesOnlyOnLeft().keySet());
                    changedProjects.addAll(difference.entriesOnlyOnRight().keySet());
                    changedProjects.addAll(difference.entriesDiffering().keySet());
                } else {
                    if (lastSimilarities != null) {
                        changedProjects.addAll(lastSimilarities.keySet());
                    }

                    if (currentSimilarities != null) {
                        changedProjects.addAll(currentSimilarities.keySet());
                    }
                }
                notifyEventBus(changedProjects);

                lastSimilarities = currentSimilarities;
                TimeUnit.MILLISECONDS.sleep(interval);
            } catch (InterruptedException e) {
                logger.error("similarity monitor error:", e);
            }
        }
    }

    public void notifyEventBus(Set<String> projects) {
        for (String project : projects) {
           SimilarityEvent event = new SimilarityEvent();
           event.setProjectName(project);
           eventBusInstance.publish(event);
        }
    }
}
