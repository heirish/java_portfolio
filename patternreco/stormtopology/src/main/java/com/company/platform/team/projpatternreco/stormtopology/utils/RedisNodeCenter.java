package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2018/7/12.
 */
public class RedisNodeCenter {
    private static final String REDIS_KEY_DELIMITER = ":";
    private static final String REDIS_KEY_PREFIX = "nelo:pattern:";
    private static final Logger logger = LoggerFactory.getLogger(RedisNodeCenter.class);

    private JedisPool jedisPool;
    private static RedisNodeCenter nodeCenter;


    private RedisNodeCenter(Map config) {
        String host = config.get("host").toString();
        int port = (int)Double.parseDouble(config.get("port").toString());

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        int maxTotal = 0;
        try {
            maxTotal = (int)Double.parseDouble(config.get("maxTotal").toString());
        } catch (Exception e) {
            maxTotal = 1000;
            logger.warn("Can not parse maxTotal from config, will use default value: " + maxTotal);
        }
        jedisPoolConfig.setMaxTotal(maxTotal);
        long maxWaitMillis = 0;
        try {
            maxWaitMillis = (long)Double.parseDouble(config.get("maxWaitMillis").toString());
        } catch (Exception e) {
            maxWaitMillis = 5000;
            logger.warn("Can not parse maxWaitMillis from config, will use default value: " + maxWaitMillis);
        }
        jedisPoolConfig.setMaxWaitMillis(maxWaitMillis);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPool = new JedisPool(jedisPoolConfig, host, port);
    }

    public static synchronized  RedisNodeCenter getInstance(Map config) {
       if (nodeCenter == null) {
          nodeCenter = new RedisNodeCenter(config);
       }
       return nodeCenter;
    }

    public Map<PatternNodeKey, PatternNode> getProjectLevelNodes(PatternLevelKey levelKey) {
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();

        Jedis redis = jedisPool.getResource();
        Map<String, String> result = new HashMap<>();
        try {
            if (levelKey == null) {
                logger.error("invalid levelKey.");
                return nodes;
            }
            String redisLevelKey = REDIS_KEY_PREFIX + levelKey.toDelimitedString(REDIS_KEY_DELIMITER);
            result = redis.hgetAll(redisLevelKey);
        } catch (Exception e) {
            logger.error("Get project level nodes error, ", e);
        } finally {
            if (redis != null) {
                redis.close();
            }
        }

        for (Map.Entry<String, String> entry : result.entrySet()) {
            try {
                String redisNodeKey = entry.getKey().substring(REDIS_KEY_PREFIX.length());
                if (!entry.getKey().startsWith(REDIS_KEY_PREFIX) || StringUtils.isEmpty(redisNodeKey)) {
                    logger.warn("invalid redis nodeKey: " + entry.getKey());
                    continue;
                }
                PatternNodeKey nodeKey = PatternNodeKey.fromDelimitedString(redisNodeKey, REDIS_KEY_DELIMITER);
                PatternNode node = PatternNode.fromJson(entry.getValue());
                nodes.put(nodeKey, node);
            } catch (Exception e) {
                logger.warn("Parse pattern Node error, nodeKey: " + entry.getKey() + ", nodeValue: " + entry.getValue(), e);
            }
        }
        return nodes;
    }

    public boolean addNode(PatternNodeKey nodeKey, PatternNode node) {
        String redisLevelKey = REDIS_KEY_PREFIX + nodeKey.getLevelKey().toDelimitedString(REDIS_KEY_DELIMITER);
        String redisNodeKey = REDIS_KEY_PREFIX + nodeKey.toDelimitedString(REDIS_KEY_DELIMITER);
        String redisNodeValue = node.toJson();

        if (existNode(nodeKey)) {
           logger.warn("Node: " + nodeKey.toString() + " already exists in redis, will overwrite it.");
        }
        Jedis redis = jedisPool.getResource();
        redis.hset(redisLevelKey, redisNodeKey, redisNodeValue);
        redis.close();

        return true;
    }

    public boolean updateNode(PatternNodeKey nodeKey, PatternNode node) {
        String redisLevelKey = REDIS_KEY_PREFIX + nodeKey.getLevelKey().toDelimitedString(REDIS_KEY_DELIMITER);
        String redisNodeKey = REDIS_KEY_PREFIX + nodeKey.toDelimitedString(REDIS_KEY_DELIMITER);
        String redisNodeValue = node.toJson();

        if (!existNode(nodeKey)) {
            logger.warn("Node: " + nodeKey.toString() + " not exist in redis, will add it.");
        }
        Jedis redis = jedisPool.getResource();
        redis.hset(redisLevelKey, redisNodeKey, redisNodeValue);
        redis.close();

        return true;
    }

    private boolean existNode(PatternNodeKey nodeKey) {
        Jedis redis = jedisPool.getResource();
        try {
            String redisLevelKey = REDIS_KEY_PREFIX + nodeKey.getLevelKey().toDelimitedString(REDIS_KEY_DELIMITER);
            String redisNodeKey = REDIS_KEY_PREFIX + nodeKey.toDelimitedString(REDIS_KEY_DELIMITER);
            String redisNode = redis.hget(redisLevelKey, redisNodeKey);
            if (!StringUtils.isEmpty(redisNode)) {
               return true;
            }
        } catch (Exception e) {
            logger.error("Get node key error, ", e);
        } finally {
            redis.close();
        }
        return false;
    }
}
