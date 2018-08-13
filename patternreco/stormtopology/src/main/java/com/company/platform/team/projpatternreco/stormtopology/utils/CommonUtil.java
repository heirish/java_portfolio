package com.company.platform.team.projpatternreco.stormtopology.utils;

import com.company.platform.team.projpatternreco.common.data.PatternNode;
import com.company.platform.team.projpatternreco.common.data.PatternNodeKey;
import com.company.platform.team.projpatternreco.stormtopology.data.Constants;
import com.company.platform.team.projpatternreco.stormtopology.data.DBProjectPatternNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/8/5 0005.
 */
public class CommonUtil {
    private static final Logger logger = LoggerFactory.getLogger(CommonUtil.class);

    public static double round(double value, int precision) {
        BigDecimal bd = new BigDecimal(value)
                .setScale(precision, RoundingMode.HALF_EVEN);
        return bd.doubleValue();
    }

    public static boolean equalWithPrecision(double value1, double value2, int precision) {
        double precisionFactor = Math.pow(10, precision);
        BigDecimal bdValue1 = new BigDecimal(value1 * precisionFactor).setScale(0, RoundingMode.HALF_EVEN);
        BigDecimal bdValue2 = new BigDecimal(value2 * precisionFactor).setScale(0, RoundingMode.HALF_EVEN);
        return StringUtils.equals(bdValue1.toString(), bdValue2.toString());
    }

    public static List<DBProjectPatternNode> formatDBPatternNodes(Map<PatternNodeKey, PatternNode> nodes, int projectID) {
        if (nodes != null && nodes.size() > 0) {
            List<DBProjectPatternNode> DBNodes = new ArrayList<>();
            for (Map.Entry<PatternNodeKey, PatternNode> node : nodes.entrySet()) {
                PatternNodeKey nodeKey = node.getKey();
                PatternNode patternNode = node.getValue();
                DBProjectPatternNode DBNode = new DBProjectPatternNode(projectID, nodeKey.getLevel(), nodeKey.getId());
                if (patternNode.hasParent()) {
                    PatternNodeKey parent = patternNode.getParentId();
                    DBNode.setParentKey(parent.getId());
                } else {
                    DBNode.setParentKey("");
                }
                DBNode.setPatternTokens(patternNode.getPatternTokens());
                DBNode.setReresentTokens(patternNode.getRepresentTokens());
                DBNodes.add(DBNode);
            }
            return DBNodes;
        }
        return null;
    }
    public static Map<PatternNodeKey, PatternNode> formatPatternNode(List<DBProjectPatternNode> nodes, String projectName) {
        if (nodes != null && !StringUtils.isEmpty(projectName)) {
            Map<PatternNodeKey, PatternNode> patternNodes = new HashMap<>();
            for (DBProjectPatternNode node : nodes) {
                try {
                    String nodeKeyString = String.format("%s%s%s%s%s", projectName, Constants.PATTERN_TOKENS_DELIMITER,
                            node.getPatternLevel(), Constants.PATTERN_TOKENS_DELIMITER,
                            node.getPatternKey());
                    PatternNodeKey nodeKey = PatternNodeKey.fromDelimitedString(nodeKeyString, Constants.PATTERN_TOKENS_DELIMITER);
                    PatternNode patternNode = new PatternNode(node.getRepresentTokens());
                    patternNodes.put(nodeKey, patternNode);
                } catch (Exception e) {
                    logger.warn("format PatternNode error.", e);
                }
            }
            return patternNodes;
        }
        return null;
    }

    public static boolean replayFailedTuple(Map conf) {
        boolean replayTuple;
        try {
            replayTuple = Boolean.parseBoolean(
                    ((Map)conf.get(Constants.CONFIGURE_TOPOLOGY_SECTION)).get("replayFailedTuple").toString());
        } catch (Exception e) {
            replayTuple = false;
            logger.error("get replay failed tuple from conf failed, use default value: " + replayTuple);
        }
        return replayTuple;
    }
}
