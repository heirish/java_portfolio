package com.company.platform.team.projpatternreco.stormtopology.utils;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPartitioner implements Partitioner {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinPartitioner.class);
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final int MAX_COUNTER_VALUE = 65536;
    private static final int INITIAL_COUNTER_VALUE = 0;
    private static Random random = new Random();

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        try {
            int partitionId = counter.getAndIncrement() % partitions.size();
            if (counter.get() > MAX_COUNTER_VALUE) {
                counter.set(INITIAL_COUNTER_VALUE);
            }
            return partitionId;
        } catch (Exception e) {
            logger.error("error occur : ", e);
            try {
                return random.nextInt(random.nextInt(partitions.size()));
            } catch (Exception ex) {
                logger.error("error occur using random : ", ex);
                return 0;
            }
        }
    }

    @Override
    public void close() {
    }
}
