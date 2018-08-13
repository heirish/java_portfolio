package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.common.data.PatternLevelKey;
import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.*;

/**
 * Created by admin on 2018/7/12.
 */
public class RedisUtil {
    private static final String REDIS_KEY_DELIMITER = ":";
    private static final String REDIS_KEY_PATTERN_PREFIX = "nelo:pattern:nodes:";
    private static final String REDIS_KEY_META_PREFIX = "nelo:pattern:meta:";
    private static final Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    private JedisPool jedisPool;
    private static RedisUtil nodeCenter;


    private RedisUtil(Map config) {
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

    public static synchronized RedisUtil getInstance(Map config) {
       if (nodeCenter == null) {
          nodeCenter = new RedisUtil(config);
       }
       return nodeCenter;
    }

    public PatternNodeKey addNode(PatternLevelKey levelKey, PatternNode node) {
        PatternNodeKey nodeKey = new PatternNodeKey(levelKey);
        String redisLevelKey = REDIS_KEY_PATTERN_PREFIX + nodeKey.getLevelKey().toDelimitedString(REDIS_KEY_DELIMITER);
        String redisNodeKey = REDIS_KEY_PATTERN_PREFIX + nodeKey.toDelimitedString(REDIS_KEY_DELIMITER);
        String redisNodeValue = node.toJson();

        setHashFieldValue(redisLevelKey, redisNodeKey, redisNodeValue);
        return nodeKey;
    }

    public void deleteProjectNodes(String projectName) {
        String keyPattern = REDIS_KEY_PATTERN_PREFIX + projectName + REDIS_KEY_DELIMITER + "*";
        Set<String> keySets = scanKeysByPattern(keyPattern);

        Jedis redis = jedisPool.getResource();
        try {
            for (String key: keySets) {
                redis.del(key);
            }
        } catch (Exception e) {
            logger.error("delete project nodes error.", e);
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
        logger.info("delete " + keySets.size() + " keys for project " + projectName);
    }

    public boolean updateNode(PatternNodeKey nodeKey, PatternNode node) {
        String redisLevelKey = REDIS_KEY_PATTERN_PREFIX + nodeKey.getLevelKey().toDelimitedString(REDIS_KEY_DELIMITER);
        String redisNodeKey = REDIS_KEY_PATTERN_PREFIX + nodeKey.toDelimitedString(REDIS_KEY_DELIMITER);
        String redisNodeValue = node.toJson();

        setHashFieldValue(redisLevelKey, redisNodeKey, redisNodeValue);
        return true;
    }

    public PatternNode getNode(PatternNodeKey nodeKey) {
        String redisLevelKey = REDIS_KEY_PATTERN_PREFIX + nodeKey.getLevelKey().toDelimitedString(REDIS_KEY_DELIMITER);
        String redisNodeKey = REDIS_KEY_PATTERN_PREFIX + nodeKey.toDelimitedString(REDIS_KEY_DELIMITER);

        PatternNode node = null;
        try {
            String result = getHashFieldValue(redisLevelKey, redisNodeKey);
            if (result != null) {
                node = PatternNode.fromJson(result);
            }
        } catch (Exception e) {
            logger.error("get node: " + redisNodeKey + " from redis error", e);
        }

        return node;
    }
    public Map<PatternNodeKey, PatternNode> getLevelNewNodes(Set<PatternNodeKey> localNodeKeys, PatternLevelKey levelKey) {
        Map<String, String> result = new HashMap<>();
        Map<PatternNodeKey, PatternNode> nodes = new HashMap<>();
        if (levelKey == null) {
            return nodes;
        }

        String redisKey =  REDIS_KEY_PATTERN_PREFIX + levelKey.toDelimitedString(REDIS_KEY_DELIMITER);
        Jedis redis = jedisPool.getResource();
        try {
            result = redis.hgetAll(redisKey);
        } catch (Exception e) {
            logger.error("Get level " + levelKey.toString() + "'s values error", e);
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
        if (result == null  || result.size() == 0) {
            logger.warn("Can not find fields for key: " + redisKey);
            return nodes;
        }

        for (Map.Entry<String, String> entry : result.entrySet()) {
            try {
                String redisNodeKey = entry.getKey().substring(REDIS_KEY_PATTERN_PREFIX.length());
                if (!entry.getKey().startsWith(REDIS_KEY_PATTERN_PREFIX) || StringUtils.isEmpty(redisNodeKey)) {
                    logger.warn("invalid redis nodeKey: " + entry.getKey());
                    continue;
                }

                PatternNodeKey nodeKey = PatternNodeKey.fromDelimitedString(redisNodeKey, REDIS_KEY_DELIMITER);
                if (localNodeKeys != null && localNodeKeys.contains(nodeKey)) {
                    continue;
                }
                logger.debug("json lenth: " + entry.getValue().length());
                PatternNode node = PatternNode.fromJson(entry.getValue());
                nodes.put(nodeKey, node);
            } catch (Exception e) {
                logger.warn("Parse pattern Node error, nodeKey: " + entry.getKey() + ", nodeValue: " + entry.getValue(), e);
            }
        }
        return nodes;
    }
    public Set<String> getAllProjects() {
        Set<String> projects = new HashSet<>();
        String keyPattern = REDIS_KEY_PATTERN_PREFIX + "*";
        Set<String> levelKeys = scanKeysByPattern(keyPattern);
        for (String key : levelKeys) {
            try {
                String redisLevelKey = key.substring(REDIS_KEY_PATTERN_PREFIX.length());
                PatternLevelKey levelKey = PatternLevelKey.fromDelimitedString(redisLevelKey, REDIS_KEY_DELIMITER);
                projects.add(levelKey.getProjectName());
            } catch (Exception e) {
                logger.error("parse levelKey: " + key + " failed.", e);
            }
        }

        return projects;
    }
    public long getLevelNodeSize(PatternLevelKey levelKey) {
        String redisKey =  REDIS_KEY_PATTERN_PREFIX + levelKey.toDelimitedString(REDIS_KEY_DELIMITER);
        Jedis redis = jedisPool.getResource();
        try {
            return redis.hlen(redisKey);
        } catch (Exception e) {
            logger.error("Get level " + levelKey.toString() + "'s length error", e);
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
        return -1;
    }

    public String getMetaData(String projectName, String metaString) {
        String key = REDIS_KEY_META_PREFIX + projectName + REDIS_KEY_DELIMITER + metaString;

        Jedis redis = jedisPool.getResource();
        try {
            return redis.get(key);
        } catch (Exception e) {
            logger.error("get key: " + key + "'s value error", e);
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
        return null;
    }
    public void setMetaData(String projectName, String metaString, String value) {
        String key = REDIS_KEY_META_PREFIX + projectName + REDIS_KEY_DELIMITER + metaString;

        Jedis redis = jedisPool.getResource();
        try {
            redis.set(key, value);
        } catch (Exception e) {
            logger.error("set key: " + key + "'s value error", e);
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
    }

    private Set<String> scanKeysByPattern(String pattern) {
        ScanParams scanParams = new ScanParams();
        scanParams.match(pattern);
        scanParams.count(5000);
        String index = "0";
        Set<String> keySets = new HashSet<String>();
        Jedis redis = jedisPool.getResource();
        try {
            do {
                ScanResult<String> scanResult = redis.scan(index, scanParams);
                index = scanResult.getStringCursor();
                keySets.addAll(scanResult.getResult());
            } while (!index.equalsIgnoreCase("0"));
        } catch (Exception e) {
            logger.error("scan keys error " , e);
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
        logger.info("get " + keySets.size() + " keys for pattern " + pattern);
        return keySets;
    }

    private void setHashFieldValue(String hashKey, String hashField, String value) {
        Jedis redis = jedisPool.getResource();
        try {
            if (redis.hset(hashKey, hashField, value) == 0) {
                logger.warn("Field: " + hashField + " already exists in redis, overwrite it.");
            }
        } catch (Exception e) {
            logger.error("set value for hash key: " + hashKey + ", field: " +  hashField + "failed.");
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
    }
    private String getHashFieldValue(String hashKey, String hashField) {
        Jedis redis = jedisPool.getResource();
        String result = null;
        try {
            result = redis.hget(hashKey, hashField);
        } catch (Exception e) {
            logger.error("get hash key: " + hashKey + ", field: " + hashField + "'s value error", e);
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
        return result;
    }
    private void deleteHashKey(String hashKey) {
        Jedis redis = jedisPool.getResource();
        try {
            redis.del(hashKey);
        } catch (Exception e) {
            logger.error("delete hash key " + hashKey + " error", e);
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
    }
}
